// Copyright 2026 The Kstack Authors
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

// Package model holds the gqlgen-generated GraphQL models (models_gen.go).
//
// This hand-written file exists only to keep the package non-empty: gqlgen
// deletes and rewrites models_gen.go on every run, so without a permanent file
// the package would momentarily have no Go files and gqlgen couldn't load it for
// autobinding. Most GraphQL types bind directly to internal/cluster and
// internal/auth domain types (see gqlgen.yml), so no hand-written models remain.
package model
