// Copyright © 2023 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os/exec"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	tagParamName     = "json"
	tagParamDefault  = "default"
	tagParamValidate = "validate"

	validationRequired    = "required"
	validationLT          = "lt"
	validationLessThan    = "less-than"
	validationGT          = "gt"
	validationGreaterThan = "greater-than"
	validationInclusion   = "inclusion"
	validationExclusion   = "exclusion"
	validationRegex       = "regex"

	tagSeparator      = ","
	validateSeparator = "="
	listSeparator     = "|"
	fieldSeparator    = "."
)

// ParseParameters parses the struct into a map of parameter, requires the folder path that has the struct, and the
// struct name
func ParseParameters(path string, name string) (map[string]sdk.Parameter, string, error) {
	mod, err := parseModule(path)
	if err != nil {
		return nil, "", fmt.Errorf("error parsing module: %w", err)
	}
	pkg, err := parsePackage(path)
	if err != nil {
		return nil, "", fmt.Errorf("error parsing package: %w", err)
	}
	myStruct, file, err := findStruct(pkg, name)
	if err != nil {
		return nil, "", err
	}

	return (&parameterParser{
		pkg:     pkg,
		mod:     mod,
		file:    file,
		imports: map[string]*ast.Package{},
	}).Parse(myStruct)
}

type module struct {
	Path  string       // module path
	Dir   string       // directory holding files for this module, if any
	Error *moduleError // error loading module
}

type moduleError struct {
	Err string // the error itself
}

func parseModule(path string) (module, error) {
	cmd := exec.Command("go", "list", "-m", "-json")
	cmd.Dir = path
	stdout, err := cmd.StdoutPipe()

	if err != nil {
		return module{}, fmt.Errorf("error piping stdout of go list command: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return module{}, fmt.Errorf("error starting go list command: %w", err)
	}
	var mod module
	if err := json.NewDecoder(stdout).Decode(&mod); err != nil {
		return module{}, fmt.Errorf("error decoding go list output: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return module{}, fmt.Errorf("error running command %q: %w", cmd.String(), err)
	}
	if mod.Error != nil {
		return module{}, fmt.Errorf("error loading module: %s", mod.Error.Err)
	}
	return mod, nil
}

func parsePackage(path string) (*ast.Package, error) {
	fset := token.NewFileSet()
	filterTests := func(info fs.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}
	pkgs, err := parser.ParseDir(fset, path, filterTests, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse directory %s: %w", path, err)
	}
	// Make sure they are all in one package.
	if len(pkgs) == 0 {
		return nil, fmt.Errorf("no source-code package in directory %s", path)
	}
	if len(pkgs) > 1 {
		return nil, fmt.Errorf("multiple packages in directory %s", path)
	}
	for _, v := range pkgs {
		return v, nil // return first package
	}
	panic("unreachable")
}

func findStruct(pkg *ast.Package, name string) (*ast.StructType, *ast.File, error) {
	var structType *ast.StructType
	var file *ast.File
	for _, f := range pkg.Files {
		ast.Inspect(f, func(n ast.Node) bool {
			// Check if the node is a struct declaration
			if typeSpec, ok := n.(*ast.TypeSpec); ok && typeSpec.Name.String() == name {
				structType, ok = typeSpec.Type.(*ast.StructType)
				if !ok {
					// Node is not a struct declaration
					return true
				}
				file = f
				// stop iterating
				return false
			}
			// Return true to continue iterating over the ast.File
			return true
		})
	}
	if file == nil {
		return nil, nil, fmt.Errorf("struct %q was not found in the package %q", name, pkg.Name)
	}
	return structType, file, nil
}

type parameterParser struct {
	// pkg holds the current package we are working with
	pkg *ast.Package
	// file holds the current file we are working with
	file *ast.File

	mod module

	imports map[string]*ast.Package
}

func (p *parameterParser) Parse(structType *ast.StructType) (map[string]sdk.Parameter, string, error) {
	pkgName := p.pkg.Name

	parameters, err := p.parseStructType(structType, nil)
	if err != nil {
		return nil, "", err
	}

	return parameters, pkgName, nil
}

func (p *parameterParser) parseIdent(ident *ast.Ident, field *ast.Field) (params map[string]sdk.Parameter, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("[parseIdent] %w", err)
		}
	}()

	if p.isBuiltinType(ident.Name) {
		// builtin type, that's a parameter
		t := p.getParamType(ident)
		name, param, err := p.parseSingleParameter(field, t)
		if err != nil {
			return nil, err
		}
		return map[string]sdk.Parameter{name: param}, nil
	}

	if ident.Obj == nil {
		// need to find the identifier in another file
		ts, file, err := p.findType(p.pkg, ident.Name)
		if err != nil {
			return nil, err
		}

		// change the type for simplicity
		ident.Obj = &ast.Object{
			Name: ident.Name,
			Decl: ts,
		}

		// back up current file and replace it because we are now working with
		// another file, we want to revert this once we are done parsing this type
		backupFile := p.file
		p.file = file
		defer func() {
			p.file = backupFile
		}()
	}

	switch v := ident.Obj.Decl.(type) {
	case *ast.TypeSpec:
		return p.parseTypeSpec(v, field)
	default:
		return nil, fmt.Errorf("unexpected type: %T", ident.Obj.Decl)
	}
}

func (p *parameterParser) parseTypeSpec(ts *ast.TypeSpec, f *ast.Field) (params map[string]sdk.Parameter, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("[parseTypeSpec] %w", err)
		}
	}()

	switch v := ts.Type.(type) {
	case *ast.StructType:
		return p.parseStructType(v, f)
	case *ast.SelectorExpr:
		return p.parseSelectorExpr(v, f)
	case *ast.Ident:
		return p.parseIdent(v, f)
	default:
		return nil, fmt.Errorf("unexpected type: %T", ts.Type)
	}
}

func (p *parameterParser) parseStructType(st *ast.StructType, f *ast.Field) (params map[string]sdk.Parameter, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("[parseStructType] %w", err)
		}
	}()

	for _, f := range st.Fields.List {
		fieldParams, err := p.parseField(f)
		if err != nil {
			return nil, fmt.Errorf("error parsing field %q: %w", f.Names[0].Name, err)
		}
		if params == nil {
			params = fieldParams
			continue
		}
		for k, v := range fieldParams {
			if _, ok := params[k]; ok {
				return nil, fmt.Errorf("parameter %q is defined twice", k)
			}
			params[k] = v
		}
	}
	if f != nil {
		// attach prefix of field in which this struct type is declared
		params = p.attachPrefix(f, params)
	}
	return params, nil
}

// parse tags, defaults and stuff
func (p *parameterParser) parseField(f *ast.Field) (params map[string]sdk.Parameter, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("[parseField] %w", err)
		}
	}()

	if len(f.Names) == 1 && !f.Names[0].IsExported() {
		return nil, nil // ignore unexported fields
	}

	switch v := f.Type.(type) {
	case *ast.Ident:
		// identifier (builtin type or type in same package)
		return p.parseIdent(v, f)
	case *ast.StructType:
		// nested type
		return p.parseStructType(v, f)
	case *ast.SelectorExpr:
		return p.parseSelectorExpr(v, f)
	case *ast.ArrayType:
		strType := fmt.Sprintf("%s", v.Elt)
		if !p.isBuiltinType(strType) && !strings.Contains(strType, "time Duration") {
			return nil, fmt.Errorf("unsupported slice type: %s", strType)
		}

		name, param, err := p.parseSingleParameter(f, sdk.ParameterTypeString)
		if err != nil {
			return nil, err
		}
		return map[string]sdk.Parameter{name: param}, nil
	default:
		return nil, fmt.Errorf("unknown type: %T", f.Type)
	}
}

func (p *parameterParser) parseSelectorExpr(se *ast.SelectorExpr, f *ast.Field) (params map[string]sdk.Parameter, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("[parseSelectorExpr] %w", err)
		}
	}()

	imp, err := p.findImportSpec(se)
	if err != nil {
		return nil, err
	}

	if impPath := strings.Trim(imp.Path.Value, `"`); impPath == "time" && se.Sel.Name == "Duration" {
		// we allow the duration type
		name, param, err := p.parseSingleParameter(f, sdk.ParameterTypeDuration)
		if err != nil {
			return nil, err
		}
		return map[string]sdk.Parameter{name: param}, nil
	}

	// first find package
	pkg, err := p.findPackage(imp.Path.Value)
	if err != nil {
		return nil, err
	}

	// now find requested type in that package
	ts, file, err := p.findType(pkg, se.Sel.Name)
	if err != nil {
		return nil, err
	}

	// back up current file and replace it because we are now working with
	// another file, we want to revert this once we are done parsing this type
	backupFile := p.file
	backupPkg := p.pkg
	p.file = file
	p.pkg = pkg
	defer func() {
		p.file = backupFile
		p.pkg = backupPkg
	}()

	return p.parseTypeSpec(ts, f)
}

func (p *parameterParser) findPackage(importPath string) (*ast.Package, error) {
	// first cleanup string
	importPath = strings.Trim(importPath, `"`)

	if !strings.HasPrefix(importPath, p.mod.Path) {
		// we only allow types declared in the same module
		return nil, fmt.Errorf("we do not support parameters from package %v (please use builtin types or time.Duration)", importPath)
	}

	if pkg, ok := p.imports[importPath]; ok {
		// it's cached already
		return pkg, nil
	}

	pkgDir := p.mod.Dir + strings.TrimPrefix(importPath, p.mod.Path)
	pkg, err := parsePackage(pkgDir)
	if err != nil {
		return nil, fmt.Errorf("could not parse package dir %q: %w", pkgDir, err)
	}

	// cache it for future use
	p.imports[importPath] = pkg
	return pkg, nil
}

func (p *parameterParser) findType(pkg *ast.Package, typeName string) (*ast.TypeSpec, *ast.File, error) {
	var file *ast.File
	var found *ast.TypeSpec
	for _, f := range pkg.Files {
		ast.Inspect(f, func(node ast.Node) bool {
			ts, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}
			if ts.Name.Name != typeName {
				return true
			}

			// found our type, store the file and type
			file = f
			found = ts
			return false
		})
		if found != nil {
			// already found the type
			break
		}
	}
	if found == nil {
		return nil, nil, fmt.Errorf("could not find type %v in package %v", typeName, pkg.Name)
	}
	return found, file, nil
}

func (p *parameterParser) findImportSpec(se *ast.SelectorExpr) (*ast.ImportSpec, error) {
	impName := se.X.(*ast.Ident).Name
	for _, i := range p.file.Imports {
		if (i.Name != nil && i.Name.Name == impName) ||
			strings.HasSuffix(strings.Trim(i.Path.Value, `"`), impName) {
			return i, nil
		}
	}
	return nil, fmt.Errorf("could not find import %q", impName)
}

func (p *parameterParser) attachPrefix(f *ast.Field, params map[string]sdk.Parameter) map[string]sdk.Parameter {
	// attach prefix if a tag is present or if the field is named
	prefix := p.getTag(f.Tag, tagParamName)
	if prefix == "" && len(f.Names) > 0 {
		prefix = p.formatFieldName(f.Names[0].Name)
	}
	// if it's a struct, then prefix is the struct name
	if n, ok := f.Type.(*ast.Ident); ok && prefix == "" {
		prefix = p.formatFieldName(n.Name)
	}
	if prefix == "" {
		// no prefix to attach
		return params
	}

	prefixedParams := make(map[string]sdk.Parameter)
	for k, v := range params {
		prefixedParams[prefix+fieldSeparator+k] = v
	}
	return prefixedParams
}

func (p *parameterParser) isBuiltinType(name string) bool {
	switch name {
	case "string", "bool", "int", "uint", "int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64",
		"byte", "rune", "float32", "float64":
		return true
	default:
		return false
	}
}

func (p *parameterParser) parseSingleParameter(f *ast.Field, t sdk.ParameterType) (paramName string, param sdk.Parameter, err error) {
	fieldName, err := p.getFieldName(f)
	if err != nil {
		return "", sdk.Parameter{}, err
	}

	paramName = p.getTag(f.Tag, tagParamName)
	if paramName == "" {
		// if there's no tag use the formatted field paramName
		paramName = p.formatFieldName(fieldName)
	}

	var validations []sdk.Validation
	validate := p.getTag(f.Tag, tagParamValidate)
	if validate != "" {
		validations, err = p.parseValidateTag(validate)
		if err != nil {
			return "", sdk.Parameter{}, err
		}
	}

	return paramName, sdk.Parameter{
		Default:     p.getTag(f.Tag, tagParamDefault),
		Description: p.formatFieldComment(f, fieldName, paramName),
		Validations: validations,
		Type:        t,
	}, nil
}

func (p *parameterParser) getFieldName(f *ast.Field) (string, error) {
	if len(f.Names) == 1 {
		return f.Names[0].Name, nil
	}

	switch v := f.Type.(type) {
	case *ast.Ident:
		return v.Name, nil
	case *ast.SelectorExpr:
		return v.Sel.Name, nil
	default:
		return "", fmt.Errorf("unexpected type: %T", f.Type)
	}
}

func (p *parameterParser) getParamType(i *ast.Ident) sdk.ParameterType {
	switch i.Name {
	case "int8", "uint8", "int16", "uint16", "int32", "rune", "uint32", "int64", "uint64", "int", "uint":
		return sdk.ParameterTypeInt
	case "float32", "float64":
		return sdk.ParameterTypeFloat
	case "bool":
		return sdk.ParameterTypeBool
	default:
		return sdk.ParameterTypeString
	}
}

// formatFieldName formats the name to a camel case string that starts with a
// lowercase letter. If the string starts with multiple uppercase letters, all
// but the last character in the sequence will be converted into lowercase
// letters (e.g. HTTPRequest -> httpRequest).
func (p *parameterParser) formatFieldName(name string) string {
	if name == "" {
		return ""
	}
	nameRunes := []rune(name)
	foundLowercase := false
	i := 0
	newName := strings.Map(func(r rune) rune {
		if foundLowercase {
			return r
		}
		if unicode.IsLower(r) {
			// short circuit
			foundLowercase = true
			return r
		}
		if i == 0 ||
			(len(nameRunes) > i+1 && unicode.IsUpper(nameRunes[i+1])) {
			r = unicode.ToLower(r)
		}
		i++
		return r
	}, name)
	return newName
}

func (p *parameterParser) formatFieldComment(f *ast.Field, fieldName, paramName string) string {
	doc := f.Doc
	if doc == nil {
		// fallback to line comment
		doc = f.Comment
	}
	c := strings.ReplaceAll(doc.Text(), fieldName, paramName)
	if len(c) == 0 {
		return c
	}

	whitespacePrefix := ""
	for _, r := range c {
		if !unicode.IsSpace(r) {
			break
		}
		whitespacePrefix += string(r)
	}

	// get rid of whitespace in first line
	c = strings.TrimPrefix(c, whitespacePrefix)
	// get rid of whitespace in front of all other lines
	c = strings.ReplaceAll(c, "\n"+whitespacePrefix, "\n")
	// get rid of new lines and use a space instead
	c = strings.ReplaceAll(c, "\n", " ")
	// trim space (get rid of any eventual new lines at the end)
	c = strings.Trim(c, " ")
	return c
}

func (p *parameterParser) getTag(lit *ast.BasicLit, tag string) string {
	if lit == nil {
		return ""
	}

	st := reflect.StructTag(strings.Trim(lit.Value, "`"))
	return st.Get(tag)
}

func (p *parameterParser) parseValidateTag(tag string) ([]sdk.Validation, error) {
	validations := make([]sdk.Validation, 0)
	split := strings.Split(tag, tagSeparator)

	for i, s := range split {
		s = strings.TrimSpace(s)
		split[i] = s
		v, err := p.parseValidation(split[i])
		if err != nil {
			return nil, err
		}
		if v != nil {
			validations = append(validations, v)
		}
	}
	return validations, nil
}

func (p *parameterParser) parseValidation(str string) (sdk.Validation, error) {
	if str == validationRequired {
		return sdk.ValidationRequired{}, nil
	}
	split := strings.Split(str, validateSeparator)
	if len(split) != 2 {
		return nil, fmt.Errorf("invalid tag format")
	}

	switch split[0] {
	case validationRequired:
		req, err := strconv.ParseBool(split[1])
		if err != nil {
			return nil, err
		}
		// if required=false then do not add a validation
		if !req {
			return nil, nil
		}
		return sdk.ValidationRequired{}, nil
	case validationLT, validationLessThan:
		val, err := strconv.ParseFloat(split[1], 64)
		if err != nil {
			return nil, err
		}
		return sdk.ValidationLessThan{Value: val}, nil
	case validationGT, validationGreaterThan:
		val, err := strconv.ParseFloat(split[1], 64)
		if err != nil {
			return nil, err
		}
		return sdk.ValidationGreaterThan{Value: val}, nil
	case validationInclusion:
		list := strings.Split(split[1], listSeparator)
		return sdk.ValidationInclusion{List: list}, nil
	case validationExclusion:
		list := strings.Split(split[1], listSeparator)
		return sdk.ValidationExclusion{List: list}, nil
	case validationRegex:
		return sdk.ValidationRegex{Regex: regexp.MustCompile(split[1])}, nil
	default:
		return nil, fmt.Errorf("invalid value for tag validate: %s", str)
	}
}
