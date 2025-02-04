// Copyright © 2025 Meroxa, Inc.
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
	"fmt"
	"os"

	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/readmegen"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen"
	"github.com/spf13/cobra"
)

func main() {
	cmdRoot := &cobra.Command{
		Use:   "conn-sdk-cli",
		Short: "Tooling around generating connector related files",
	}

	cmdReadmegen := &cobra.Command{
		Use:   "readmegen",
		Short: "Generate README for connector",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			specifications := cmd.Flag("specifications").Value.String()
			readme := cmd.Flag("readme").Value.String()
			write, _ := cmd.Flags().GetBool("write")

			return readmegen.NewCommand(specifications, readme, write).Execute(cmd.Context())
		},
	}
	cmdReadmegen.Flags().StringP("specifications", "s", "./connector.yaml", "path to the connector.yaml file")
	cmdReadmegen.Flags().StringP("readme", "r", "./README.md", "path to the README.md file")
	cmdReadmegen.Flags().BoolP("write", "w", false, "Overwrite readme file instead of printing to stdout")

	cmdSpecgen := &cobra.Command{
		Use:   "specgen",
		Short: "Generate specification files for connector",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			output := cmd.Flag("output").Value.String()
			path := cmd.Flag("path").Value.String()

			return specgen.NewCommand(output, path).Execute(cmd.Context())
		},
	}
	cmdSpecgen.Flags().StringP("output", "o", "connector.yaml", "name of the output file")
	cmdSpecgen.Flags().StringP("path", "p", ".", "path to the package that contains the Connector variable")

	cmdRoot.AddCommand(
		cmdReadmegen,
		cmdSpecgen,
	)
	cmdRoot.CompletionOptions.DisableDefaultCmd = true

	if err := cmdRoot.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}
