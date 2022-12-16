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
	"flag"
	"log"
	"os"
	"strings"

	"github.com/conduitio/conduit-connector-sdk/cmd/paramgen/internal"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("paramgen: ")

	// parse the command arguments
	args := parseFlags()

	// parse the sdk parameters
	params, pkg, err := internal.ParseParameters(args.path, args.structName)
	if err != nil {
		log.Fatalf("paramgen failed to parse parameters: %v\n", err)
	}

	code := internal.GenerateCode(params, pkg, args.structName)

	internal.WriteCodeToFile(code, args.path, args.output)
}

type Args struct {
	output     string
	path       string
	structName string
}

func parseFlags() Args {
	flags := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	var (
		output = flags.String("output", "paramgen.go", "name of the output file")
		path   = flags.String("path", ".", "directory path to the package that has the configuration struct")
	)

	// flags is set up to exit on error, we can safely ignore the error
	_ = flags.Parse(os.Args[1:])

	if len(flags.Args()) == 0 {
		log.Fatalf("struct name should be specified")
	}

	var args Args
	args.output = stringPtrToVal(output)
	args.path = stringPtrToVal(path)
	args.structName = flags.Args()[0]

	// add .go suffix if it is not in the name
	if !strings.HasSuffix(args.output, ".go") {
		args.output += ".go"
	}

	return args
}

func stringPtrToVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
