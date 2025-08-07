-- Create a separate checkpoint table for detailed tracking
CREATE TABLE IF NOT EXISTS sync_checkpoint (
    id INT PRIMARY KEY AUTO_INCREMENT,
    symbol_id INT NOT NULL,
    interval_type VARCHAR(10) NOT NULL DEFAULT '1m',
    last_timestamp BIGINT NOT NULL,
    end_timestamp BIGINT NOT NULL,
    bars_processed INT DEFAULT 0,
    total_bars INT DEFAULT 0,
    checkpoint_data JSON DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES symbolsmap(id) ON DELETE CASCADE,
    INDEX idx_symbol_interval (symbol_id, interval_type),
    INDEX idx_last_timestamp (last_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Note: The sync_status table columns may already exist from previous partial migration
-- If they don't exist, you can manually add them with:
-- ALTER TABLE sync_status 
-- ADD COLUMN last_timestamp BIGINT DEFAULT NULL,
-- ADD COLUMN checkpoint_data JSON DEFAULT NULL,
-- ADD COLUMN interval_type VARCHAR(10) DEFAULT '1m',
-- ADD COLUMN start_time TIMESTAMP DEFAULT NULL,
-- ADD COLUMN end_time TIMESTAMP DEFAULT NULL;