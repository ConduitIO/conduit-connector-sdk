// Copyright © 2022 Meroxa, Inc.
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

package specgen

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os/exec"
	"reflect"
	"strings"
	"unicode"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	CommentPrefix = "spec"

	KeywordSummary           = "summary"
	KeywordDescription       = "description"
	KeywordVersion           = "version" // TODO remove this and extract version from git
	KeywordAuthor            = "author"
	KeywordDestinationParams = "destinationParams"
	KeywordSourceParams      = "sourceParams"

	TagParamName     = "name"
	TagParamDefault  = "default"
	TagParamRequired = "required"
)

var (
	errNotFound = errors.New("not found")
)

func ParseSpecification(path string) (sdk.Specification, error) {
	mod, err := ParseModule(path)
	if err != nil {
		return sdk.Specification{}, fmt.Errorf("error parsing module: %w", err)
	}
	pkg, err := ParsePackage(path)
	if err != nil {
		return sdk.Specification{}, fmt.Errorf("error parsing package: %w", err)
	}
	return (&specificationParser{mod: mod, imports: map[string]*ast.Package{}}).Parse(pkg)
}

type Module struct {
	Path  string       // module path
	Dir   string       // directory holding files for this module, if any
	Error *ModuleError // error loading module
}

type ModuleError struct {
	Err string // the error itself
}

func ParseModule(path string) (Module, error) {
	cmd := exec.Command("go", "list", "-m", "-json")
	cmd.Dir = path
	stdout, err := cmd.StdoutPipe()

	if err != nil {
		return Module{}, fmt.Errorf("error piping stdout of go list command: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return Module{}, fmt.Errorf("error starting go list command: %w", err)
	}
	var module Module
	if err := json.NewDecoder(stdout).Decode(&module); err != nil {
		return Module{}, fmt.Errorf("error decoding go list output: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return Module{}, fmt.Errorf("error running command %q: %w", cmd.String(), err)
	}
	if module.Error != nil {
		return Module{}, fmt.Errorf("error loading module: %s", module.Error.Err)
	}
	return module, nil
}

func ParsePackage(path string) (*ast.Package, error) {
	fset := token.NewFileSet()
	filterTests := func(info fs.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}
	pkg, err := parser.ParseDir(fset, path, filterTests, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse directory %v: %w", path, err)
	}
	if len(pkg) != 1 {
		return nil, errors.New("more than 1 package")
	}
	for _, v := range pkg {
		return v, nil // return first package
	}
	panic("unreachable")
}

type specificationParser struct {
	// pkg holds the current file that we are working on
	file *ast.File
	mod  Module

	imports map[string]*ast.Package
}

func (p *specificationParser) Parse(pkg *ast.Package) (sdk.Specification, error) {
	var spec sdk.Specification
	spec.Name = p.mod.Path

	ts, err := p.findSpecgenType(pkg)
	if err != nil {
		return sdk.Specification{}, fmt.Errorf("error finding spec struct: %w", err)
	}

	st, ok := ts.Type.(*ast.StructType)
	if !ok {
		return sdk.Specification{}, fmt.Errorf("error asserting (*ast.TypeSpec).Type: expected %T, got %T", &ast.StructType{}, ts.Type)
	}

	// parse specification fields from comment
	keywordFieldMapping := map[string]*string{
		KeywordSummary:     &spec.Summary,
		KeywordDescription: &spec.Description,
		KeywordVersion:     &spec.Version, // TODO parse version from go mod if possible, or use git
		KeywordAuthor:      &spec.Author,
	}

	for keyword, target := range keywordFieldMapping {
		*target, err = p.parseCommentGroup(ts.Doc, keyword)
		if err != nil && err != errNotFound {
			return sdk.Specification{}, err
		}
	}

	// parse source / destination params
	spec.SourceParams, err = p.parseParams(st, KeywordSourceParams)
	if err != nil { // TODO errNotFound is allowed
		return sdk.Specification{}, fmt.Errorf("error parsing soruce parameters: %w", err)
	}
	spec.DestinationParams, err = p.parseParams(st, KeywordDestinationParams)
	if err != nil {
		return sdk.Specification{}, fmt.Errorf("error parsing destination parameters: %w", err)
	}

	return spec, nil
}

// findSpecgenType finds the declaration of the type that has specgen specific
// comments. The comments that identified this TypeSpec can be found in the
// field Doc.
func (p *specificationParser) findSpecgenType(pkg *ast.Package) (*ast.TypeSpec, error) {
	var typeSpec *ast.TypeSpec
	for _, f := range pkg.Files {
		var lastErr error
		ast.Inspect(f, func(n ast.Node) bool {
			gd, ok := n.(*ast.GenDecl)
			if !ok {
				return true
			}

			var ts *ast.TypeSpec
			if gd.Lparen == token.NoPos {
				// this is a declaration in one line
				// first make sure it's a type declaration
				ts, ok = gd.Specs[0].(*ast.TypeSpec)
				if !ok {
					return true
				}

				// only one spec in type group, the comment should be on the
				// generic declaration
				_, err := p.parseCommentGroup(gd.Doc, KeywordSummary)
				if err == errNotFound {
					return true
				} else if err != nil {
					lastErr = err
					return false
				}

				// copy over docs from generic declaration to spec, we know it
				// doesn't have any docs because the generic declaration has no
				// parentheses (we abuse the system, but we use it for good :) )
				ts.Doc = gd.Doc
			} else {
				// it's a group declaration, check types inside
				found := false
				for _, s := range gd.Specs {
					// make sure it's a type declaration
					ts, ok = s.(*ast.TypeSpec)
					if !ok {
						// not a type declaration, we can stop here
						return true
					}

					// since we can have multiple types in the group the comment
					// should be on the type
					_, err := p.parseCommentGroup(ts.Doc, KeywordSummary)
					if err == errNotFound {
						continue // check other types
					} else if err != nil {
						lastErr = err
						return false
					}
					found = true
					break
				}
				if !found {
					return true
				}
			}

			if typeSpec != nil {
				// we found two structs that have a summary keyword
				lastErr = fmt.Errorf(
					"found two structs with a comment that starts with %q (%s and %s), please make sure only one struct has this comment",
					p.formatCommentPrefix(KeywordSummary),
					typeSpec.Name.Name,
					ts.Name.Name,
				)
				return false
			}
			typeSpec = ts
			p.file = f
			return true
		})
		if lastErr != nil {
			return nil, lastErr
		}
	}
	if typeSpec == nil {
		return nil, fmt.Errorf("could not find any struct with a comment that starts with %q, please define such a struct", p.formatCommentPrefix(KeywordSummary))
	}
	return typeSpec, nil
}

func (p *specificationParser) parseCommentGroup(cg *ast.CommentGroup, keyword string) (string, error) {
	if cg == nil {
		return "", errNotFound
	}

	specPrefix := p.formatCommentPrefix("")
	keywordPrefix := p.formatCommentPrefix(keyword)

	found := false
	var buf strings.Builder
loop:
	for _, c := range cg.List {
		switch {
		case strings.HasPrefix(c.Text, "//"+keywordPrefix+" ") || c.Text == "//"+keywordPrefix:
			if found {
				// we already found another line with the keyword prefix, not expected
				return "", fmt.Errorf("found two comment lines with prefix %q, please remove one and try again", keywordPrefix)
			}
			found = true
			text := strings.TrimPrefix(c.Text, "//")
			text = strings.TrimPrefix(text, keywordPrefix)
			text = strings.TrimSpace(text)
			if _, err := buf.WriteString(text); err != nil {
				panic(err) // according to the docs WriteString always returns a nil error
			}
		case strings.HasPrefix(c.Text, "//"+specPrefix):
			if found {
				// we already found the comment we were looking for and
				// encountered another spec comment, we are done parsing
				break loop
			}
		case found:
			// it's a comment without a spec prefix and we already found the
			// keyword we are looking for, we regard this as a multiline comment
			text := strings.TrimPrefix(c.Text, "//")
			if !strings.HasPrefix(text, "  ") {
				// users might want to use spaces to indent text in multiple lines
				// in case there are multiple spaces we don't remove them, otherwise
				// we trim the space
				text = strings.TrimSpace(text)
			}

			if buf.Len() > 0 {
				_, _ = buf.WriteRune('\n') // according to the docs WriteRune always returns a nil error
			}
			_, _ = buf.WriteString(text) // according to the docs WriteString always returns a nil error
		}
	}
	if !found {
		return "", errNotFound
	}

	result := buf.String()
	result = strings.TrimRight(result, "\n")
	return result, nil
}

func (*specificationParser) formatCommentPrefix(keyword string) string {
	return fmt.Sprintf("%s:%s", CommentPrefix, keyword)
}

func (p *specificationParser) parseParams(specgenStruct *ast.StructType, keyword string) (map[string]sdk.Parameter, error) {
	var field *ast.Field
	for _, f := range specgenStruct.Fields.List {
		_, err := p.parseCommentGroup(f.Doc, keyword)
		if err == errNotFound {
			continue
		} else if err != nil {
			return nil, err
		}
		if field != nil {
			return nil, fmt.Errorf(
				"found two fields with the comment %q, please make sure only one field has this comment",
				p.formatCommentPrefix(keyword),
			)
		}
		field = f
	}
	if field == nil {
		return nil, errNotFound
	}

	return (*paramsParser)(p).parse(field)
}

// paramsParser groups functions that concern themselves with parsing source and
// destination parameters.
type paramsParser specificationParser

// parse takes a field that represents a source or destination config and parses
// the parameters by recursively traversing the type in that field.
func (p *paramsParser) parse(f *ast.Field) (map[string]sdk.Parameter, error) {
	ident, ok := f.Type.(*ast.Ident)
	if !ok {
		return nil, fmt.Errorf("error asserting (*ast.Field).Type: expected %T, got %T", &ast.Ident{}, f.Type)
	}
	params, err := p.parseIdent(ident)
	if err != nil {
		return nil, fmt.Errorf("error parsing parameters: %w", err)
	}
	return params, nil
}

func (p *paramsParser) parseIdent(ident *ast.Ident) (params map[string]sdk.Parameter, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("[parseIdent] %w", err)
		}
	}()

	switch v := ident.Obj.Decl.(type) {
	case *ast.TypeSpec:
		return p.parseTypeSpec(v)
	default:
		return nil, fmt.Errorf("unexpected type: %T", ident.Obj.Decl)
	}
}

func (p *paramsParser) parseTypeSpec(ts *ast.TypeSpec) (params map[string]sdk.Parameter, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("[parseTypeSpec] %w", err)
		}
	}()

	switch v := ts.Type.(type) {
	case *ast.StructType:
		return p.parseStructType(v)
	default:
		return nil, fmt.Errorf("unexpected type: %T", ts.Type)
	}
}

func (p *paramsParser) parseStructType(st *ast.StructType) (params map[string]sdk.Parameter, err error) {
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
	return params, nil
}

func (p *paramsParser) parseField(f *ast.Field) (params map[string]sdk.Parameter, err error) {
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
		if v.Obj == nil {
			// builtin type, that's a parameter
			name, param, err := p.parseSingleParameter(f)
			if err != nil {
				return nil, err
			}
			return map[string]sdk.Parameter{name: param}, nil
		}

		params, err = p.parseIdent(v)
		if err != nil {
			return nil, err
		}
		return p.attachPrefix(f, params), nil
	case *ast.StructType:
		// nested type
		params, err = p.parseStructType(v)
		if err != nil {
			return nil, err
		}
		return p.attachPrefix(f, params), nil
	case *ast.SelectorExpr:
		// imported type
		imp, err := p.findImportSpec(v)
		if err != nil {
			return nil, err
		}

		impPath := strings.Trim(imp.Path.Value, `"`)
		switch {
		case impPath == "time" && v.Sel.Name == "Duration":
			// we allow the duration type
			name, param, err := p.parseSingleParameter(f)
			if err != nil {
				return nil, err
			}
			return map[string]sdk.Parameter{name: param}, nil
		default:
			params, err = p.parseSelectorExpr(v)
			if err != nil {
				return nil, err
			}
			return p.attachPrefix(f, params), nil
		}
	default:
		return nil, fmt.Errorf("unknown type: %T", f.Type)
	}
}

func (p *paramsParser) parseSelectorExpr(se *ast.SelectorExpr) (params map[string]sdk.Parameter, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("[parseSelectorExpr] %w", err)
		}
	}()

	imp, err := p.findImportSpec(se)
	if err != nil {
		return nil, err
	}

	impName := se.X.(*ast.Ident).Name
	impPath := strings.Trim(imp.Path.Value, `"`)

	if !strings.HasPrefix(impPath, p.mod.Path) {
		// we only allow types declared in the same module
		// edge case: could be in a submodule, but let's disregard that for now
		return nil, fmt.Errorf("we do not support parameters of type %v.%v", impName, se.Sel.Name)
	}

	// TODO parse package
	return nil, nil
}

func (p *paramsParser) findImportSpec(se *ast.SelectorExpr) (*ast.ImportSpec, error) {
	impName := se.X.(*ast.Ident).Name
	for _, i := range p.file.Imports {
		if (i.Name != nil && i.Name.Name == impName) ||
			strings.HasSuffix(strings.Trim(i.Path.Value, `"`), impName) {
			return i, nil
		}
	}
	return nil, fmt.Errorf("could not find import %q", impName)
}

func (p *paramsParser) attachPrefix(f *ast.Field, params map[string]sdk.Parameter) map[string]sdk.Parameter {
	// attach prefix if a tag is present or if the field is named
	prefix := p.getTag(f.Tag, TagParamName)
	if prefix == "" && len(f.Names) > 0 {
		prefix = p.formatFieldName(f.Names[0].Name)
	}
	if prefix == "" {
		// no prefix to attach
		return params
	}

	prefixedParams := make(map[string]sdk.Parameter)
	for k, v := range params {
		prefixedParams[prefix+"."+k] = v
	}
	return prefixedParams
}

func (p *paramsParser) parseSingleParameter(f *ast.Field) (name string, param sdk.Parameter, err error) {
	var fieldName string
	if len(f.Names) == 1 {
		fieldName = f.Names[0].Name
	} else {
		switch v := f.Type.(type) {
		case *ast.Ident:
			fieldName = v.Name
		case *ast.SelectorExpr:
			fieldName = v.Sel.Name
		default:
			return "", sdk.Parameter{}, fmt.Errorf("unexpected type: %T", f.Type)
		}
	}

	name = p.getTag(f.Tag, TagParamName)
	if name == "" {
		// if there's no tag use the formatted field name
		name = p.formatFieldName(fieldName)
	}

	// replace field name with parameter name in description so that the user
	// can write normal go docs referencing the field name
	desc := strings.Replace(f.Doc.Text(), fieldName, name, -1)

	return name, sdk.Parameter{
		Default:     p.getTag(f.Tag, TagParamDefault),
		Required:    p.getTag(f.Tag, TagParamRequired) == "true",
		Description: desc,
		// TODO parse param type once we add it to the SDK
	}, nil
}

// formatFieldName formats the name to a camel case string that starts with a
// lowercase letter. If the string starts with multiple uppercase letters, all
// but the last character in the sequence will be converted into lowercase
// letters (e.g. HTTPRequest -> httpRequest).
func (p *paramsParser) formatFieldName(name string) string {
	if name == "" {
		return ""
	}
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
			(len(name) > i+1 && unicode.IsUpper(rune(name[i+1]))) {
			r = unicode.ToLower(r)
		}
		i++
		return r
	}, name)
	return newName
}

func (p *paramsParser) getTag(lit *ast.BasicLit, tag string) string {
	if lit == nil {
		return ""
	}

	st := reflect.StructTag(strings.Trim(lit.Value, "`"))
	return st.Get(tag)
}
