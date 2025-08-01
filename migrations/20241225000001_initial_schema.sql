-- Migration: Initial Schema
-- Created: 2024-12-25 00:00:01

-- +migrate Up
-- Create initial database schema for trading system

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

-- +migrate Down
-- Drop all tables in reverse order
DROP TABLE IF EXISTS system_config;
DROP TABLE IF EXISTS user_sessions;
DROP TABLE IF EXISTS trading_sessions;
DROP TABLE IF EXISTS market_watch;
DROP TABLE IF EXISTS symbolsmap;