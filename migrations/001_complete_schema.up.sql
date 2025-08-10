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
    instrument_type ENUM('SPOT', 'FUTURES', 'OPTIONS') DEFAULT 'SPOT',
    base_currency VARCHAR(50),
    quote_currency VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    min_price_increment DECIMAL(20,10),
    min_quantity_increment DECIMAL(20,10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_exchange_symbol (exchange, symbol),
    INDEX idx_active (is_active),
    INDEX idx_currencies (base_currency, quote_currency)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================
-- 2. TRADING SESSIONS TABLE
-- =====================================
CREATE TABLE IF NOT EXISTS trading_sessions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL,
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    timezone VARCHAR(50) NOT NULL,
    days_active JSON,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    INDEX idx_sync_status (sync_status)
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
-- 8. INSERT DEFAULT DATA
-- =====================================

-- Insert default trading sessions
INSERT IGNORE INTO trading_sessions (name, start_time, end_time, timezone, days_active) VALUES
('Asian', '00:00:00', '08:00:00', 'UTC', '["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]'),
('London', '08:00:00', '16:00:00', 'UTC', '["monday", "tuesday", "wednesday", "thursday", "friday"]'),
('New York', '13:00:00', '21:00:00', 'UTC', '["monday", "tuesday", "wednesday", "thursday", "friday"]'),
('Crypto 24/7', '00:00:00', '23:59:59', 'UTC', '["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]');

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

-- Insert some default active symbols (examples - can be customized)
INSERT IGNORE INTO symbolsmap (exchange, symbol, full_name, instrument_type, base_currency, quote_currency, is_active) VALUES
('binance', 'BTCUSDT', 'Bitcoin/USDT', 'SPOT', 'BTC', 'USDT', TRUE),
('binance', 'ETHUSDT', 'Ethereum/USDT', 'SPOT', 'ETH', 'USDT', TRUE),
('binance', 'BNBUSDT', 'Binance Coin/USDT', 'SPOT', 'BNB', 'USDT', TRUE),
('binance', 'SOLUSDT', 'Solana/USDT', 'SPOT', 'SOL', 'USDT', TRUE),
('binance', 'XRPUSDT', 'Ripple/USDT', 'SPOT', 'XRP', 'USDT', TRUE);