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
        // string; ClusterID is an opaque decimal-string id (an int64 server-side,
        // a string on the wire) — the webview treats it as an opaque string.
        scalars: {
          Time: 'string',
          ClusterID: 'string',
        },
      },
    },
  },
};

export default config;
