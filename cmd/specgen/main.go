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

package main

import (
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"log"
	"reflect"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/davecgh/go-spew/spew"
	"golang.org/x/tools/go/packages"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("specgen: ")
	flag.Parse()

	// We accept either one directory or a list of files. Which do we have?
	args := flag.Args()
	if len(args) == 0 {
		// Default: process whole package in current directory.
		args = []string{"."}
	}

	pkg := parsePackage(args, nil)
	spec := (*Package)(pkg).parseSpec()
	spew.Dump(spec)
}

type Package packages.Package

func (p *Package) parseSpec() Specification {
	var spec Specification
	spec.Name = p.Module.Path
	for _, f := range p.Syntax {
		ast.Inspect(f, func(n ast.Node) bool {
			var commentGroup *ast.CommentGroup
			switch x := n.(type) {
			case *ast.FuncDecl:
				commentGroup = x.Doc
			case *ast.TypeSpec:
				commentGroup = x.Doc
			case *ast.Field:
				commentGroup = x.Doc
			case *ast.GenDecl:
				commentGroup = x.Doc
			}
			if commentGroup == nil || !p.containsSpec(commentGroup) {
				return true
			}
			err := p.parseNode(n, commentGroup, &spec)
			if err != nil {
				log.Fatal(err)
			}

			return true
		})
	}
	return spec
}

func (p *Package) containsSpec(cg *ast.CommentGroup) bool {
	for _, c := range cg.List {
		if strings.HasPrefix(c.Text, "// @spec") {
			return true
		}
	}
	return false
}

func (p *Package) parseNode(n ast.Node, cg *ast.CommentGroup, spec *Specification) error {
	for i, c := range cg.List {
		// TODO parse keyword and use switch instead
		if strings.HasPrefix(c.Text, "// @spec summary ") {
			val := p.parseComment(cg.List[i:], "summary")
			err := spec.SetSummary(val)
			if err != nil {
				return err
			}
		}
		if strings.HasPrefix(c.Text, "// @spec description ") {
			val := p.parseComment(cg.List[i:], "description")
			err := spec.SetDescription(val)
			if err != nil {
				return err
			}
		}
		if strings.HasPrefix(c.Text, "// @spec author ") {
			val := p.parseComment(cg.List[i:], "author")
			err := spec.SetAuthor(val)
			if err != nil {
				return err
			}
		}
		if strings.HasPrefix(c.Text, "// @spec version ") {
			val := p.parseComment(cg.List[i:], "version")
			err := spec.SetVersion(val)
			if err != nil {
				return err
			}
		}
		if strings.HasPrefix(c.Text, "// @spec sourceparams") {
			f, ok := n.(*ast.Field)
			if !ok {
				return errors.New("expected @spec sourceparams to be on field")
			}
			params := p.parseParams(f.Type)
			err := spec.SetSourceParams(params)
			if err != nil {
				return err
			}
		}
		if strings.HasPrefix(c.Text, "// @spec destinationparams") {
			f, ok := n.(*ast.Field)
			if !ok {
				return errors.New("expected @spec destinationparams to be on field")
			}
			params := p.parseParams(f.Type)
			err := spec.SetDestinationParams(params)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Package) parseParams(expr ast.Expr) map[string]sdk.Parameter {
	params := make(map[string]sdk.Parameter)
	switch x := expr.(type) {
	case *ast.SelectorExpr:
		// imported from another package
		ident, ok := x.X.(*ast.Ident)
		if !ok {
			log.Fatal("expected ident")
		}
		importedPkg := p.Imports[ident.Name]
		if importedPkg == nil {
			for _, imp := range p.Types.Imports() {
				if imp.Name() == ident.Name {
					importedPkg = parsePackage([]string{imp.Path()}, nil)
					break
				}
			}
			if importedPkg == nil {
				log.Fatalf("could not import %v", ident.Name)
			}
			p.Imports[ident.Name] = importedPkg // store for later if we need it
		}

		for _, f := range importedPkg.Syntax {
			ast.Inspect(f, func(node ast.Node) bool {
				typeSpec, ok := node.(*ast.TypeSpec)
				if !ok {
					return true
				}
				if typeSpec.Name.Name != x.Sel.Name {
					return true
				}
				params = (*Package)(importedPkg).parseParams(typeSpec.Type)
				return false
			})
		}

	case *ast.Ident:
		if x.Obj == nil {
			break
		}
		// same package
		typeSpec, ok := x.Obj.Decl.(*ast.TypeSpec)
		if !ok {
			log.Fatal("expected type spec")
		}
		params = p.parseParams(typeSpec.Type)
	case *ast.StructType:
		for _, field := range x.Fields.List {
			var param sdk.Parameter
			if se, ok := field.Type.(*ast.SelectorExpr); ok {
				// imported struct again, parse separately
				innerParams := p.parseParams(se)
				for k, v := range innerParams {
					params[k] = v
				}
				continue
			}
			// TODO might not be a named field, this will panic
			name := field.Names[0].Name
			param.Description = field.Doc.Text() // TODO change start of comment from struct field name to input field name
			if field.Tag != nil {
				tag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
				if val, ok := tag.Lookup("default"); ok {
					param.Default = val
				}
				if val, ok := tag.Lookup("validations"); ok {
					if val == "required" {
						param.Required = true
					}
				}
			}
			params[name] = param
		}
	}
	return params
}

func (p *Package) parseComment(cg []*ast.Comment, specKeyword string) string {
	buf := &strings.Builder{}
	if _, err := buf.WriteString(strings.TrimPrefix(cg[0].Text, fmt.Sprintf("// @spec %v ", specKeyword))); err != nil {
		log.Fatal(err)
	}
	if len(cg) > 1 {
		p.parseMultiline(cg[1:], buf)
	}
	return buf.String()
}

func (p *Package) parseMultiline(cg []*ast.Comment, buf *strings.Builder) {
	for _, c := range cg {
		if !strings.HasPrefix(c.Text, "// | ") {
			return
		}
		buf.WriteString(" ")
		buf.WriteString(strings.TrimPrefix(c.Text, "// | "))
	}
}

// parsePackage analyzes the single package constructed from the patterns and tags.
// parsePackage exits if there is an error.
func parsePackage(patterns []string, tags []string) *packages.Package {
	cfg := &packages.Config{
		Mode: packages.NeedName |
			packages.NeedFiles |
			packages.NeedCompiledGoFiles |
			packages.NeedTypes |
			packages.NeedSyntax |
			packages.NeedTypesInfo |
			packages.NeedTypesSizes |
			packages.NeedModule,
		Tests:      false,
		BuildFlags: []string{fmt.Sprintf("-tags=%s", strings.Join(tags, " "))},
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		log.Fatal(err)
	}
	if len(pkgs) != 1 {
		log.Fatal("more than 1 package")
	}
	pkgs[0].Imports = make(map[string]*packages.Package)
	return pkgs[0]
}

type Specification sdk.Specification

func (s *Specification) SetName(v string) error {
	if s.Name != "" {
		return errors.New("Specification.Name already set! Check if you already specified it in the comments.")
	}
	s.Name = v
	return nil
}
func (s *Specification) SetSummary(v string) error {
	if s.Summary != "" {
		return errors.New("Specification.Summary already set! Check if you already specified it in the comments.")
	}
	s.Summary = v
	return nil
}
func (s *Specification) SetDescription(v string) error {
	if s.Description != "" {
		return errors.New("Specification.Description already set! Check if you already specified it in the comments.")
	}
	s.Description = v
	return nil
}
func (s *Specification) SetVersion(v string) error {
	if s.Version != "" {
		return errors.New("Specification.Version already set! Check if you already specified it in the comments.")
	}
	s.Version = v
	return nil
}
func (s *Specification) SetAuthor(v string) error {
	if s.Author != "" {
		return errors.New("Specification.Author already set! Check if you already specified it in the comments.")
	}
	s.Author = v
	return nil
}
func (s *Specification) SetDestinationParams(v map[string]sdk.Parameter) error {
	if s.DestinationParams != nil {
		return errors.New("Specification.DestinationParams already set! Check if you already specified it in the comments.")
	}
	s.DestinationParams = v
	return nil
}
func (s *Specification) SetSourceParams(v map[string]sdk.Parameter) error {
	if s.SourceParams != nil {
		return errors.New("Specification.SourceParams already set! Check if you already specified it in the comments.")
	}
	s.SourceParams = v
	return nil
}
