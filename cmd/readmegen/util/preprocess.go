// Copyright © 2024 Meroxa, Inc.
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

package util

import (
	"errors"
	"fmt"
	"strings"
)

const (
	readmegenTagPrefix = "readmegen:"
	readmegenOpenTag   = "<!-- " + readmegenTagPrefix
	readmegenCloseTag  = "<!-- /" + readmegenTagPrefix

	readmegenOpenTagLength  = len(readmegenOpenTag)
	readmegenCloseTagLength = len(readmegenCloseTag)

	commentClose       = "-->"
	commentCloseLength = len(commentClose)
)

var (
	preprocessTags = map[string]string{
		"name":        `{{ title .specification.Name }}`,
		"summary":     `{{ .specification.Summary }}`,
		"description": `{{ .specification.Description }}`,
		"version":     `{{ .specification.Version }}`,
		"author":      `{{ .specification.Author }}`,

		"source.parameters.yaml":       `{{ template "parameters.yaml" args "specification" .specification "parameters" .sourceParams }}`,
		"source.parameters.table":      `{{ template "parameters.table" args "specification" .specification "parameters" .sourceParams }}`,
		"destination.parameters.yaml":  `{{ template "parameters.yaml" args "specification" .specification "parameters" .destinationParams }}`,
		"destination.parameters.table": `{{ template "parameters.table" args "specification" .specification "parameters" .destinationParams }}`,
	}
	errReadmegenCommentNotFound = errors.New("readmegen open tag not found")
)

// Preprocess takes the contents of a readme file and preprocesses it by replacing
// readmegen tags with the corresponding values. A readmegen tag is a HTML comment
// with the following format: <!-- readmegen:tag --> which has a corresponding
// end tag <!-- /readmegen:tag -->. Everything between the tags is replaced with
// the corresponding value. If the tag is not found, or a comment is not closed,
// an error is returned.
func Preprocess(data string) (string, error) {
	var out strings.Builder
	for {
		comment, err := nextReadmegenComment(data)
		if errors.Is(err, errReadmegenCommentNotFound) {
			// no more readmegen comments, flush the rest of the data
			_, _ = out.WriteString(data)
			break
		}
		if err != nil {
			return "", err
		}

		tmpl, ok := preprocessTags[comment.tag]
		if !ok {
			return "", errors.New("unknown readmegen tag: " + comment.tag)
		}

		_, _ = out.WriteString(fmt.Sprintf("%s%s%s", data[:comment.openEndIndex], tmpl, data[comment.closeStartIndex:comment.closeEndIndex]))
		data = data[comment.closeEndIndex:]
	}
	return out.String(), nil
}

func nextReadmegenComment(data string) (readmegenComment, error) {
	openStartIndex := strings.Index(data, readmegenOpenTag)
	if openStartIndex == -1 {
		return readmegenComment{}, errReadmegenCommentNotFound
	}
	openEndIndex := strings.Index(data[openStartIndex:], commentClose)
	if openEndIndex == -1 {
		return readmegenComment{}, errors.New("readmegen open tag not closed")
	}
	openEndIndex += openStartIndex + commentCloseLength

	tag := strings.TrimRight(data[openStartIndex+readmegenOpenTagLength:openEndIndex-commentCloseLength], " ")

	closeStartIndex := strings.Index(data[openEndIndex:], readmegenCloseTag+tag)
	if closeStartIndex == -1 {
		return readmegenComment{}, errors.New("readmegen close tag not found")
	}
	closeStartIndex += openEndIndex

	closeEndIndex := strings.Index(data[closeStartIndex:], commentClose)
	if closeEndIndex == -1 {
		return readmegenComment{}, errors.New("readmegen close tag not closed")
	}
	closeEndIndex += closeStartIndex + commentCloseLength

	// check if the close tag contains only the tag
	if strings.TrimRight(data[closeStartIndex+readmegenCloseTagLength:closeEndIndex-commentCloseLength], " ") != tag {
		return readmegenComment{}, errors.New("readmegen close tag not found")
	}

	return readmegenComment{
		openStartIndex:  openStartIndex,
		openEndIndex:    openEndIndex,
		closeStartIndex: closeStartIndex,
		closeEndIndex:   closeEndIndex,
		tag:             tag,
	}, nil
}

type readmegenComment struct {
	openStartIndex  int
	openEndIndex    int
	closeStartIndex int
	closeEndIndex   int
	tag             string
}
