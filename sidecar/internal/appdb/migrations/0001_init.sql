-- app.db is the durable, app-level SQLite database (one schema_migrations
-- sequence, owned by internal/appdb). It currently holds no app-level tables:
-- cluster records and their spec/status live in the beehive store
-- (<data-dir>/beehive.db), which is the single source of truth for clusters.
--
-- This initial migration intentionally creates nothing. It exists only to anchor
-- the migration sequence at version 1 so the first real app-level table lands as
-- 0002_*.sql. Add new app-level tables as new numbered migrations here; do not
-- reintroduce a clusters table — that would be a second source of truth.
SELECT 1;
