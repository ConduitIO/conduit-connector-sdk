// Copyright © 2026 Meroxa, Inc.
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

// Package schema gives connectors access to Conduit's schema registry. A
// connector uses it to register the schema of the records it produces (a
// source) or to fetch the schema of the records it receives (a destination),
// so that record payloads and keys can be encoded and decoded consistently
// across a pipeline.
//
// The main entry points are [Create], which registers a schema under a subject
// and returns it with an assigned ID and version, and [Get], which retrieves a
// previously registered schema by subject and version. Both delegate to
// [Service], the package-level schema service client.
//
// # Service resolution
//
// [Service] defaults to an in-memory implementation, which is what runs when a
// connector is exercised in tests or built into Conduit. When a connector runs
// as a standalone plugin, the SDK calls Init during startup (registered through
// the internal standalone-utilities mechanism) and swaps Service for a
// gRPC-backed client that talks to Conduit's schema registry. In both cases the
// service is wrapped in a cache that memoizes Create and Get responses for 15
// minutes, so repeatedly registering or fetching the same schema on the hot
// path is cheap.
//
// # Schema contexts
//
// A schema context namespaces subjects so that connectors running in different
// contexts do not collide on subject names. Use [WithSchemaContextName] to put
// a context name on the [context.Context]; [Create] then prefixes the subject
// with it (as "<context>:<subject>"). [GetSchemaContextName] reads it back.
//
// # Invariants
//
//   - Only Avro is currently supported. Creating a schema with any other type
//     returns an error.
//   - Versions are assigned by the registry, start at 1, and increase by one
//     with each new schema registered under the same subject.
//
// This package re-exports the core schema types (see [Type], [Schema], [Serde])
// from github.com/conduitio/conduit-commons/schema so connectors do not have to
// import that package directly. The source and destination schema-extraction
// middleware in the parent sdk package build on these helpers, in particular
// [AttachKeySchemaToRecord] and [AttachPayloadSchemaToRecord].
package schema
