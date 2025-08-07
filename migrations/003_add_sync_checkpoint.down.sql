-- Drop the checkpoint table
DROP TABLE IF EXISTS sync_checkpoint;

-- Remove the added columns from sync_status
ALTER TABLE sync_status 
DROP COLUMN last_timestamp,
DROP COLUMN checkpoint_data,
DROP COLUMN interval_type,
DROP COLUMN start_time,
DROP COLUMN end_time;

-- Drop the index
DROP INDEX idx_sync_status_symbol_status ON sync_status;