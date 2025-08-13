-- Complete Database Schema for Trading System
-- Created: 2025-01-09
-- This single migration file contains the complete database schema

-- =====================================
-- 1. SYMBOLS MAPPING TABLE
-- =====================================
CREATE TABLE IF NOT EXISTS symbolsmap (
    id INT PRIMARY KEY AUTO_INCREMENT,
    exchange VARCHAR(20) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    full_name VARCHAR(100),
    instrument_type ENUM('SPOT', 'FUTURES', 'OPTIONS', 'FOREX', 'CFD') DEFAULT 'SPOT',
    base_currency VARCHAR(50),
    quote_currency VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    min_price_increment DECIMAL(20,10),
    min_quantity_increment DECIMAL(20,10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_exchange_symbol (exchange, symbol),
    INDEX idx_symbol (symbol),
    INDEX idx_active (is_active),
    INDEX idx_symbol_active (symbol, is_active),
    INDEX idx_currencies (base_currency, quote_currency)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================
-- 2. TRADING SESSIONS TABLE
-- =====================================
CREATE TABLE IF NOT EXISTS trading_sessions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL,
    market_type ENUM('FOREX', 'CRYPTO', 'STOCK', 'ALL') DEFAULT 'ALL',
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    timezone VARCHAR(50) NOT NULL,
    days_active JSON,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_market_type (market_type),
    INDEX idx_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================
-- 3. USER SESSIONS TABLE
-- =====================================
CREATE TABLE IF NOT EXISTS user_sessions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    session_token VARCHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    INDEX idx_token (session_token),
    INDEX idx_active (is_active, last_activity)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================
-- 4. SYSTEM CONFIGURATION TABLE
-- =====================================
CREATE TABLE IF NOT EXISTS system_config (
    id INT PRIMARY KEY AUTO_INCREMENT,
    config_key VARCHAR(100) NOT NULL UNIQUE,
    config_value TEXT,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================
-- 5. SYNC STATUS TABLE
-- =====================================
CREATE TABLE IF NOT EXISTS sync_status (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol_id INT NOT NULL,
    status ENUM('pending', 'syncing', 'completed', 'failed') NOT NULL DEFAULT 'pending',
    progress INT NOT NULL DEFAULT 0,
    total_bars INT NOT NULL DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_symbol (symbol_id),
    FOREIGN KEY (symbol_id) REFERENCES symbolsmap(id) ON DELETE CASCADE,
    INDEX idx_status (status),
    INDEX idx_symbol_status (symbol_id, status),
    INDEX idx_updated_at (updated_at),
    INDEX idx_status_progress (status, progress)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================
-- 6. MARKET WATCH TABLE
-- =====================================
CREATE TABLE IF NOT EXISTS market_watch (
    id INT PRIMARY KEY AUTO_INCREMENT,
    symbol_id INT NOT NULL,
    session_token VARCHAR(64) NOT NULL,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Sync status columns for tracking per-user symbol sync
    sync_status ENUM('pending', 'syncing', 'completed', 'failed') DEFAULT 'pending',
    sync_progress INT DEFAULT 0,
    total_bars INT DEFAULT 0,
    sync_started_at TIMESTAMP NULL,
    sync_completed_at TIMESTAMP NULL,
    FOREIGN KEY (symbol_id) REFERENCES symbolsmap(id) ON DELETE CASCADE,
    UNIQUE KEY uk_token_symbol (session_token, symbol_id),
    INDEX idx_token (session_token),
    INDEX idx_symbol_id (symbol_id),
    INDEX idx_sync_status (sync_status),
    INDEX idx_sync_status_progress (sync_status, sync_progress)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================
-- 7. SYNC CHECKPOINT TABLE
-- =====================================
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

-- =====================================
-- 8. ASSET EXTREMES TABLE
-- =====================================
CREATE TABLE IF NOT EXISTS asset_extremes (
    id INT PRIMARY KEY AUTO_INCREMENT,
    symbol_id INT NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    ath DECIMAL(20,10) COMMENT 'All-time high',
    atl DECIMAL(20,10) COMMENT 'All-time low',
    ath_date TIMESTAMP NULL COMMENT 'Date of ATH',
    atl_date TIMESTAMP NULL COMMENT 'Date of ATL',
    week_52_high DECIMAL(20,10) COMMENT '52-week high',
    week_52_low DECIMAL(20,10) COMMENT '52-week low',
    month_high DECIMAL(20,10) COMMENT '30-day high',
    month_low DECIMAL(20,10) COMMENT '30-day low',
    day_high DECIMAL(20,10) COMMENT '24-hour high',
    day_low DECIMAL(20,10) COMMENT '24-hour low',
    data_source VARCHAR(50) COMMENT 'api, historical, realtime',
    confidence_score DECIMAL(5,2) DEFAULT 0.00 COMMENT 'Data confidence 0-100',
    last_calculated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_symbol_exchange (symbol_id, exchange),
    INDEX idx_symbol_id (symbol_id),
    INDEX idx_last_calculated (last_calculated),
    INDEX idx_exchange (exchange),
    INDEX idx_symbol_last_calc (symbol_id, last_calculated),
    FOREIGN KEY (symbol_id) REFERENCES symbolsmap(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================
-- 9. INSERT DEFAULT DATA
-- =====================================

-- Insert default trading sessions
INSERT IGNORE INTO trading_sessions (name, market_type, start_time, end_time, timezone, days_active) VALUES
-- Forex Market Sessions (Monday to Friday only)
('Sydney', 'FOREX', '21:00:00', '06:00:00', 'UTC', '["sunday", "monday", "tuesday", "wednesday", "thursday"]'),
('Tokyo', 'FOREX', '00:00:00', '09:00:00', 'UTC', '["monday", "tuesday", "wednesday", "thursday", "friday"]'),
('London', 'FOREX', '08:00:00', '17:00:00', 'UTC', '["monday", "tuesday", "wednesday", "thursday", "friday"]'),
('New York', 'FOREX', '13:00:00', '22:00:00', 'UTC', '["monday", "tuesday", "wednesday", "thursday", "friday"]'),
-- Crypto Sessions (24/7)
('Crypto 24/7', 'CRYPTO', '00:00:00', '23:59:59', 'UTC', '["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]');

-- Insert default system configurations
INSERT IGNORE INTO system_config (config_key, config_value, description) VALUES
('max_symbols_per_connection', '200', 'Maximum symbols per WebSocket connection to exchange'),
('price_update_interval_ms', '100', 'Minimum interval between price updates in milliseconds'),
('connection_timeout_seconds', '30', 'WebSocket connection timeout in seconds'),
('max_reconnect_attempts', '10', 'Maximum reconnection attempts before giving up'),
('cache_ttl_seconds', '300', 'Redis cache TTL in seconds'),
('enigma_calculation_interval', '5', 'Enigma levels calculation interval in seconds'),
('binary_protocol_enabled', 'true', 'Enable binary protocol for WebSocket connections'),
('batch_size', '50', 'Number of price updates to batch together'),
('batch_interval_ms', '50', 'Interval in milliseconds to send batched updates'),
('historical_data_days', '365', 'Number of days of historical data to fetch'),
('influx_retention_days', '730', 'Number of days to retain data in InfluxDB');