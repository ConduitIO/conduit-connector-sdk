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

package sdk

import (
	"encoding/json"
	"fmt"
	"github.com/Masterminds/sprig/v3"
	"strings"
	"text/template"

	"github.com/conduitio/conduit-connector-sdk/kafkaconnect"
)

// RecordFormatter is a type that can format a record to bytes. It's used in
// destination connectors to change the output structure and format.
type RecordFormatter interface {
	Name() string
	Configure(string) (RecordFormatter, error)
	Format(Record) ([]byte, error)
}

var (
	defaultConverter = OpenCDCConverter{}
	defaultEncoder   = JSONEncoder{}
	defaultFormatter = GenericRecordFormatter{
		Converter: defaultConverter,
		Encoder:   defaultEncoder,
	}
)

const (
	genericRecordFormatSeparator     = "/" // e.g. opencdc/json
	recordFormatOptionsSeparator     = "," // e.g. opt1=val1,opt2=val2
	recordFormatOptionsPairSeparator = "=" // e.g. opt1=val1
)

// GenericRecordFormatter is a formatter that uses a Converter and Encoder to
// format a record.
type GenericRecordFormatter struct {
	Converter
	Encoder
}

// Converter is a type that can change the structure of a Record. It's used in
// destination connectors to change the output structure (e.g. opencdc records,
// debezium records etc.).
type Converter interface {
	Name() string
	Configure(map[string]string) (Converter, error)
	Convert(Record) (any, error)
}

// Encoder is a type that can encode a random struct into a byte slice. It's
// used in destination connectors to encode records into different formats
// (e.g. JSON, Avro etc.).
type Encoder interface {
	Name() string
	Configure(options map[string]string) (Encoder, error)
	Encode(r any) ([]byte, error)
}

// Name returns the name of the record formatter combined from the converter
// name and encoder name.
func (rf GenericRecordFormatter) Name() string {
	return rf.Converter.Name() + genericRecordFormatSeparator + rf.Encoder.Name()
}

func (rf GenericRecordFormatter) Configure(optRaw string) (RecordFormatter, error) {
	opt := rf.parseFormatOptions(optRaw)

	var err error
	rf.Converter, err = rf.Converter.Configure(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to configure converter: %w", err)
	}
	rf.Encoder, err = rf.Encoder.Configure(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to configure encoder: %w", err)
	}
	return rf, nil
}

func (rf GenericRecordFormatter) parseFormatOptions(options string) map[string]string {
	options = strings.TrimSpace(options)
	if len(options) == 0 {
		return nil
	}

	pairs := strings.Split(options, recordFormatOptionsSeparator)
	optMap := make(map[string]string, len(pairs))
	for _, pairStr := range pairs {
		pair := strings.SplitN(pairStr, recordFormatOptionsPairSeparator, 2)
		k := pair[0]
		v := ""
		if len(pair) == 2 {
			v = pair[1]
		}
		optMap[k] = v
	}
	return optMap
}

// Format converts and encodes record into a byte array.
func (rf GenericRecordFormatter) Format(r Record) ([]byte, error) {
	converted, err := rf.Converter.Convert(r)
	if err != nil {
		return nil, fmt.Errorf("converter fail: %w", err)
	}

	out, err := rf.Encoder.Encode(converted)
	if err != nil {
		return nil, fmt.Errorf("encoder fail: %w", err)
	}

	return out, nil
}

// OpenCDCConverter outputs an OpenCDC record (it does not change the structure
// of the record).
type OpenCDCConverter struct{}

func (c OpenCDCConverter) Name() string                                   { return "opencdc" }
func (c OpenCDCConverter) Configure(map[string]string) (Converter, error) { return c, nil }
func (c OpenCDCConverter) Convert(r Record) (any, error) {
	return r, nil
}

// DebeziumConverter outputs a Debezium record.
type DebeziumConverter struct {
	SchemaName string
}

func (c DebeziumConverter) Name() string { return "debezium" }
func (c DebeziumConverter) Configure(opt map[string]string) (Converter, error) {
	// allow user to configure the schema name (needed to make the output record
	// play nicely with Kafka Connect connectors)
	c.SchemaName = opt["debezium.schema.name"]
	return c, nil
}
func (c DebeziumConverter) Convert(r Record) (any, error) {
	before, err := c.getStructuredData(r.Payload.Before)
	if err != nil {
		return nil, err
	}
	after, err := c.getStructuredData(r.Payload.After)
	if err != nil {
		return nil, err
	}

	// we ignore the error, if the timestamp is not there milliseconds will be 0 which is fine
	var readAtMillis int64
	if readAt, err := r.Metadata.GetReadAt(); err == nil {
		readAtMillis = readAt.UnixMilli()
	}

	dbz := kafkaconnect.DebeziumPayload{
		Before:          before,
		After:           after,
		Source:          r.Metadata,
		Op:              c.getDebeziumOp(r.Operation),
		TimestampMillis: readAtMillis,
		Transaction:     nil,
	}

	e := dbz.ToEnvelope()
	e.Schema.Name = c.SchemaName
	return e, nil
}

func (c DebeziumConverter) getStructuredData(d Data) (StructuredData, error) {
	switch d := d.(type) {
	case nil:
		return nil, nil
	case StructuredData:
		return d, nil
	case RawData:
		if len(d) == 0 {
			return nil, nil
		}
		return c.parseRawDataAsJSON(d)
	default:
		return nil, fmt.Errorf("unknown data type: %T", d)
	}
}
func (c DebeziumConverter) parseRawDataAsJSON(d RawData) (StructuredData, error) {
	// We have raw data, we need structured data.
	// We can do our best and try to convert it if RawData is carrying raw JSON.
	var sd StructuredData
	err := json.Unmarshal(d, &sd)
	if err != nil {
		return nil, fmt.Errorf("could not convert RawData to StructuredData: %w", err)
	}
	return sd, nil
}
func (c DebeziumConverter) getDebeziumOp(o Operation) kafkaconnect.DebeziumOp {
	switch o {
	case OperationCreate:
		return kafkaconnect.DebeziumOpCreate
	case OperationUpdate:
		return kafkaconnect.DebeziumOpUpdate
	case OperationDelete:
		return kafkaconnect.DebeziumOpDelete
	case OperationSnapshot:
		return kafkaconnect.DebeziumOpRead
	}
	return "" // invalid operation
}

// JSONEncoder is an Encoder that outputs JSON.
type JSONEncoder struct{}

func (e JSONEncoder) Name() string                                 { return "json" }
func (e JSONEncoder) Configure(map[string]string) (Encoder, error) { return e, nil }
func (e JSONEncoder) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

// TemplateRecordFormatter is a RecordFormatter that formats a record using a Go
// template.
type TemplateRecordFormatter struct {
	template *template.Template
}

func (e TemplateRecordFormatter) Name() string { return "template" }
func (e TemplateRecordFormatter) Configure(tmpl string) (RecordFormatter, error) {
	t := template.New("")
	t = t.Funcs(sprig.FuncMap()) // inject sprig functions
	t, err := t.Parse(tmpl)
	if err != nil {
		return nil, err
	}

	e.template = t
	return e, nil
}
func (e TemplateRecordFormatter) Format(r Record) ([]byte, error) {
	var b bytes.Buffer
	err := e.template.Execute(&b, r)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
