-- Migration for sync status columns (already manually created)
-- Just add/recreate the index
DROP INDEX IF EXISTS idx_sync_status ON market_watch;
CREATE INDEX idx_sync_status ON market_watch(sync_status);