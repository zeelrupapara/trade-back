-- Rollback Complete Database Schema for Trading System
-- This migration drops all tables in reverse order to handle foreign key dependencies

-- Drop tables in reverse order of creation to handle foreign key constraints
DROP TABLE IF EXISTS sync_checkpoint;
DROP TABLE IF EXISTS market_watch;
DROP TABLE IF EXISTS sync_status;
DROP TABLE IF EXISTS system_config;
DROP TABLE IF EXISTS user_sessions;
DROP TABLE IF EXISTS trading_sessions;
DROP TABLE IF EXISTS symbolsmap;