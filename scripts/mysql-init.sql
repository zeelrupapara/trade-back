-- Initial database setup for trading system
USE trading;

-- Create symbols mapping table
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
) ENGINE=InnoDB;

-- Create market watch table
CREATE TABLE IF NOT EXISTS market_watch (
    id INT PRIMARY KEY AUTO_INCREMENT,
    symbol_id INT NOT NULL,
    session_token VARCHAR(64) NOT NULL,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES symbolsmap(id) ON DELETE CASCADE,
    UNIQUE KEY uk_token_symbol (session_token, symbol_id),
    INDEX idx_token (session_token)
) ENGINE=InnoDB;

-- Create trading sessions table
CREATE TABLE IF NOT EXISTS trading_sessions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL,
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    timezone VARCHAR(50) NOT NULL,
    days_active JSON,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Insert initial popular crypto symbols
INSERT IGNORE INTO symbolsmap (exchange, symbol, full_name, base_currency, quote_currency, min_price_increment, min_quantity_increment) VALUES
('BINANCE', 'BTCUSDT', 'Bitcoin/USDT', 'BTC', 'USDT', 0.01, 0.00001),
('BINANCE', 'ETHUSDT', 'Ethereum/USDT', 'ETH', 'USDT', 0.01, 0.0001),
('BINANCE', 'ADAUSDT', 'Cardano/USDT', 'ADA', 'USDT', 0.0001, 0.1),
('BINANCE', 'BNBUSDT', 'Binance Coin/USDT', 'BNB', 'USDT', 0.01, 0.001),
('BINANCE', 'XRPUSDT', 'XRP/USDT', 'XRP', 'USDT', 0.0001, 0.1),
('BINANCE', 'SOLUSDT', 'Solana/USDT', 'SOL', 'USDT', 0.01, 0.001),
('BINANCE', 'DOTUSDT', 'Polkadot/USDT', 'DOT', 'USDT', 0.001, 0.01),
('BINANCE', 'DOGEUSDT', 'Dogecoin/USDT', 'DOGE', 'USDT', 0.00001, 1),
('BINANCE', 'AVAXUSDT', 'Avalanche/USDT', 'AVAX', 'USDT', 0.001, 0.01),
('BINANCE', 'MATICUSDT', 'Polygon/USDT', 'MATIC', 'USDT', 0.0001, 0.1);

-- Insert trading sessions
INSERT IGNORE INTO trading_sessions (name, start_time, end_time, timezone, days_active) VALUES
('Asian', '00:00:00', '08:00:00', 'UTC', '["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]'),
('London', '08:00:00', '16:00:00', 'UTC', '["monday", "tuesday", "wednesday", "thursday", "friday"]'),
('New York', '13:00:00', '21:00:00', 'UTC', '["monday", "tuesday", "wednesday", "thursday", "friday"]'),
('Crypto 24/7', '00:00:00', '23:59:59', 'UTC', '["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]');

-- Create user sessions table for WebSocket connections
CREATE TABLE IF NOT EXISTS user_sessions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    session_token VARCHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    INDEX idx_token (session_token),
    INDEX idx_active (is_active, last_activity)
) ENGINE=InnoDB;

-- Create system configuration table
CREATE TABLE IF NOT EXISTS system_config (
    id INT PRIMARY KEY AUTO_INCREMENT,
    config_key VARCHAR(100) NOT NULL UNIQUE,
    config_value TEXT,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Insert default system configurations
INSERT IGNORE INTO system_config (config_key, config_value, description) VALUES
('max_symbols_per_connection', '200', 'Maximum symbols per WebSocket connection to exchange'),
('price_update_interval_ms', '100', 'Minimum interval between price updates in milliseconds'),
('connection_timeout_seconds', '30', 'WebSocket connection timeout in seconds'),
('max_reconnect_attempts', '10', 'Maximum reconnection attempts before giving up'),
('cache_ttl_seconds', '300', 'Redis cache TTL in seconds'),
('enigma_calculation_interval', '5', 'Enigma levels calculation interval in seconds');

-- Show created tables
SHOW TABLES;