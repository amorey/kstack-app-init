// graphql-codegen config. Reads the sidecar's authoritative schema from
// the Go module and emits typed document helpers under src/gql/ via the
// client preset (consumed by urql).
import type { CodegenConfig } from '@graphql-codegen/cli';

const config: CodegenConfig = {
  schema: 'sidecar/graph/schema.graphqls',
  // Tests are excluded: they don't define typed documents, and transport-level
  // tests use raw gql tags with schema-less placeholder operations.
  documents: ['src/**/*.{ts,tsx}', '!src/gql/**', '!src/**/*.test.{ts,tsx}'],
  ignoreNoDocuments: true,
  generates: {
    'src/gql/': {
      preset: 'client',
      config: {
        useTypeImports: true,
        // Custom scalars and their TS wire types. Time is an ISO-8601 UTC
        // string; ObjectID is an opaque decimal-string object id (an int64
        // server-side, a string on the wire) — the webview treats it as an
        // opaque string. JSON is an arbitrary decoded value (a cached object's
        // native body) — typed `unknown`, cast to a typed Kubernetes object at
        // the point of use (ObjectsTable's per-kind column registry).
        scalars: {
          Time: 'string',
          ObjectID: 'string',
          JSON: 'unknown',
        },
      },
    },
  },
};

export default config;
