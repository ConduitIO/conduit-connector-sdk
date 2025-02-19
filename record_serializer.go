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
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-sdk/kafkaconnect"
	"github.com/goccy/go-json"
)

// RecordSerializer is a type that can format a record to bytes. It's used in
// destination connectors to change the output structure and format.
type RecordSerializer interface {
	Name() string
	Configure(string) (RecordSerializer, error)
	opencdc.RecordSerializer
}

var (
	defaultConverter  = OpenCDCConverter{}
	defaultEncoder    = JSONEncoder{}
	defaultSerializer = GenericRecordSerializer{
		Converter: defaultConverter,
		Encoder:   defaultEncoder,
	}
)

const (
	genericRecordFormatSeparator     = "/" // e.g. opencdc/json
	recordFormatOptionsSeparator     = "," // e.g. opt1=val1,opt2=val2
	recordFormatOptionsPairSeparator = "=" // e.g. opt1=val1
)

// GenericRecordSerializer is a serializer that uses a Converter and Encoder to
// serialize a record.
type GenericRecordSerializer struct {
	Converter
	Encoder
}

// Converter is a type that can change the structure of a Record. It's used in
// destination connectors to change the output structure (e.g. opencdc records,
// debezium records etc.).
type Converter interface {
	Name() string
	Configure(map[string]string) (Converter, error)
	Convert(opencdc.Record) (any, error)
}

// Encoder is a type that can encode a random struct into a byte slice. It's
// used in destination connectors to encode records into different formats
// (e.g. JSON, Avro etc.).
type Encoder interface {
	Name() string
	Configure(options map[string]string) (Encoder, error)
	Encode(r any) ([]byte, error)
}

// Name returns the name of the record serializer combined from the converter
// name and encoder name.
func (rf GenericRecordSerializer) Name() string {
	return rf.Converter.Name() + genericRecordFormatSeparator + rf.Encoder.Name()
}

func (rf GenericRecordSerializer) Configure(optRaw string) (RecordSerializer, error) {
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

func (rf GenericRecordSerializer) parseFormatOptions(options string) map[string]string {
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

// Serialize converts and encodes record into a byte array.
func (rf GenericRecordSerializer) Serialize(r opencdc.Record) ([]byte, error) {
	converted, err := rf.Converter.Convert(r)
	if err != nil {
		return nil, fmt.Errorf("converter %s failed: %w", rf.Converter.Name(), err)
	}

	out, err := rf.Encoder.Encode(converted)
	if err != nil {
		return nil, fmt.Errorf("encoder %s failed: %w", rf.Encoder.Name(), err)
	}

	return out, nil
}

// OpenCDCConverter outputs an OpenCDC record (it does not change the structure
// of the record).
type OpenCDCConverter struct {
	PositionExcluded bool
}

func (c OpenCDCConverter) Name() string { return "opencdc" }
func (c OpenCDCConverter) Configure(opt map[string]string) (Converter, error) {
	positionExcludedStr, ok := opt["position.excluded"]
	if !ok {
		positionExcludedStr = "false"
	}

	positionExcluded, err := strconv.ParseBool(positionExcludedStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse position.excluded: %w", err)
	}

	c.PositionExcluded = positionExcluded
	return c, nil
}
func (c OpenCDCConverter) Convert(r opencdc.Record) (any, error) {
	if c.PositionExcluded {
		r.Position = nil
	}
	return r, nil
}

// DebeziumConverter outputs a Debezium record.
type DebeziumConverter struct {
	SchemaName string
	RawDataKey string
}

const debeziumDefaultRawDataKey = "opencdc.rawData"

func (c DebeziumConverter) Name() string { return "debezium" }
func (c DebeziumConverter) Configure(opt map[string]string) (Converter, error) {
	// allow user to configure the schema name (needed to make the output record
	// play nicely with Kafka Connect connectors)
	c.SchemaName = opt["debezium.schema.name"]
	c.RawDataKey = opt["debezium.rawData.key"]
	if c.RawDataKey == "" {
		c.RawDataKey = debeziumDefaultRawDataKey
	}
	return c, nil
}

func (c DebeziumConverter) Convert(r opencdc.Record) (any, error) {
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

func (c DebeziumConverter) getStructuredData(d opencdc.Data) (opencdc.StructuredData, error) {
	switch d := d.(type) {
	case nil:
		return nil, nil //nolint:nilnil // nil is a valid value for structured data
	case opencdc.StructuredData:
		return d, nil
	case opencdc.RawData:
		if len(d) == 0 {
			return nil, nil //nolint:nilnil // nil is a valid value for structured data
		}
		sd, err := c.parseRawDataAsJSON(d)
		if err != nil {
			// we have actually raw data, fall back to artificial structured
			// data by hoisting it into a field
			sd = opencdc.StructuredData{c.RawDataKey: d.Bytes()}
		}
		return sd, nil
	default:
		return nil, fmt.Errorf("unknown data type: %T", d)
	}
}

func (c DebeziumConverter) parseRawDataAsJSON(d opencdc.RawData) (opencdc.StructuredData, error) {
	// We have raw data, we need structured data.
	// We can do our best and try to convert it if RawData is carrying raw JSON.
	var sd opencdc.StructuredData
	err := json.Unmarshal(d, &sd)
	if err != nil {
		return nil, fmt.Errorf("could not convert RawData to StructuredData: %w", err)
	}
	return sd, nil
}

func (c DebeziumConverter) getDebeziumOp(o opencdc.Operation) kafkaconnect.DebeziumOp {
	switch o {
	case opencdc.OperationCreate:
		return kafkaconnect.DebeziumOpCreate
	case opencdc.OperationUpdate:
		return kafkaconnect.DebeziumOpUpdate
	case opencdc.OperationDelete:
		return kafkaconnect.DebeziumOpDelete
	case opencdc.OperationSnapshot:
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

// TemplateRecordSerializer is a RecordSerializer that serializes a record using
// a Go template.
type TemplateRecordSerializer struct {
	template *template.Template
}

func (e TemplateRecordSerializer) Name() string { return "template" }
func (e TemplateRecordSerializer) Configure(tmpl string) (RecordSerializer, error) {
	t := template.New("")
	t = t.Funcs(sprig.TxtFuncMap()) // inject sprig functions
	t, err := t.Parse(tmpl)
	if err != nil {
		return nil, err
	}

	e.template = t
	return e, nil
}

func (e TemplateRecordSerializer) Serialize(r opencdc.Record) ([]byte, error) {
	var b bytes.Buffer
	err := e.template.Execute(&b, r)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
