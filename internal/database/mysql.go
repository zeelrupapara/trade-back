package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/models"
)

// MySQLClient handles MySQL database operations
type MySQLClient struct {
	db     *sql.DB
	logger *logrus.Entry
	cfg    *config.MySQLConfig
}

// NewMySQLClient creates a new MySQL client
func NewMySQLClient(cfg *config.MySQLConfig, logger *logrus.Logger) (*MySQLClient, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true",
		cfg.Username,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)
	
	logger.WithField("dsn", fmt.Sprintf("%s:***@tcp(%s:%d)/%s", cfg.Username, cfg.Host, cfg.Port, cfg.Database)).Debug("Connecting to MySQL")
	
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}
	
	// Configure connection pool
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	
	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}
	
	return &MySQLClient{
		db:     db,
		logger: logger.WithField("component", "mysql"),
		cfg:    cfg,
	}, nil
}

// Query executes a query
func (mc *MySQLClient) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return mc.db.Query(query, args...)
}

// Close closes the database connection
func (mc *MySQLClient) Close() error {
	return mc.db.Close()
}

// Health checks database health
func (mc *MySQLClient) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	
	return mc.db.PingContext(ctx)
}

// Symbol operations

// GetSymbols retrieves all active symbols
func (mc *MySQLClient) GetSymbols(ctx context.Context) ([]*models.SymbolInfo, error) {
	mc.logger.Info("GetSymbols called - START")
	
	// Check if db connection exists
	if mc.db == nil {
		mc.logger.Error("Database connection is nil")
		return nil, fmt.Errorf("database connection is nil")
	}
	
	query := `
		SELECT id, exchange, symbol, full_name, instrument_type, 
		       base_currency, quote_currency, is_active, 
		       min_price_increment, min_quantity_increment,
		       created_at, updated_at
		FROM symbolsmap 
		WHERE is_active = 1
		ORDER BY symbol
	`
	
	mc.logger.Info("Executing symbols query")
	rows, err := mc.db.QueryContext(ctx, query)
	if err != nil {
		mc.logger.WithError(err).Error("Failed to query symbols")
		return nil, fmt.Errorf("failed to query symbols: %w", err)
	}
	defer rows.Close()
	
	mc.logger.Info("Query executed successfully, scanning rows...")
	
	var symbols []*models.SymbolInfo
	rowCount := 0
	for rows.Next() {
		rowCount++
		symbol := &models.SymbolInfo{}
		err := rows.Scan(
			&symbol.ID,
			&symbol.Exchange,
			&symbol.Symbol,
			&symbol.FullName,
			&symbol.InstrumentType,
			&symbol.BaseCurrency,
			&symbol.QuoteCurrency,
			&symbol.IsActive,
			&symbol.MinPriceIncrement,
			&symbol.MinQuantityIncrement,
			&symbol.CreatedAt,
			&symbol.UpdatedAt,
		)
		if err != nil {
			mc.logger.WithError(err).WithField("row", rowCount).Error("Failed to scan symbol")
			return nil, fmt.Errorf("failed to scan symbol at row %d: %w", rowCount, err)
		}
		symbols = append(symbols, symbol)
		
		// Log every 100 rows
		if rowCount % 100 == 0 {
			mc.logger.WithField("rows_scanned", rowCount).Debug("Scanning rows...")
		}
	}
	
	if err := rows.Err(); err != nil {
		mc.logger.WithError(err).Error("Error iterating rows")
		return nil, err
	}
	
	mc.logger.WithField("count", len(symbols)).Info("GetSymbols completed successfully")
	return symbols, nil
}

// GetSymbol retrieves a single symbol by exchange and symbol name
func (mc *MySQLClient) GetSymbol(ctx context.Context, exchange, symbol string) (*models.SymbolInfo, error) {
	query := `
		SELECT id, exchange, symbol, full_name, instrument_type, 
		       base_currency, quote_currency, is_active, 
		       min_price_increment, min_quantity_increment,
		       created_at, updated_at
		FROM symbolsmap 
		WHERE exchange = ? AND symbol = ?
	`
	
	symbolInfo := &models.SymbolInfo{}
	err := mc.db.QueryRowContext(ctx, query, exchange, symbol).Scan(
		&symbolInfo.ID,
		&symbolInfo.Exchange,
		&symbolInfo.Symbol,
		&symbolInfo.FullName,
		&symbolInfo.InstrumentType,
		&symbolInfo.BaseCurrency,
		&symbolInfo.QuoteCurrency,
		&symbolInfo.IsActive,
		&symbolInfo.MinPriceIncrement,
		&symbolInfo.MinQuantityIncrement,
		&symbolInfo.CreatedAt,
		&symbolInfo.UpdatedAt,
	)
	
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get symbol: %w", err)
	}
	
	return symbolInfo, nil
}

// InsertSymbol inserts a new symbol
func (mc *MySQLClient) InsertSymbol(ctx context.Context, symbol *models.SymbolInfo) error {
	query := `
		INSERT INTO symbolsmap (
			exchange, symbol, full_name, instrument_type,
			base_currency, quote_currency, is_active,
			min_price_increment, min_quantity_increment
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			full_name = VALUES(full_name),
			instrument_type = VALUES(instrument_type),
			base_currency = VALUES(base_currency),
			quote_currency = VALUES(quote_currency),
			is_active = VALUES(is_active),
			min_price_increment = VALUES(min_price_increment),
			min_quantity_increment = VALUES(min_quantity_increment),
			updated_at = CURRENT_TIMESTAMP
	`
	
	result, err := mc.db.ExecContext(ctx, query,
		symbol.Exchange,
		symbol.Symbol,
		symbol.FullName,
		symbol.InstrumentType,
		symbol.BaseCurrency,
		symbol.QuoteCurrency,
		symbol.IsActive,
		symbol.MinPriceIncrement,
		symbol.MinQuantityIncrement,
	)
	
	if err != nil {
		return fmt.Errorf("failed to insert symbol: %w", err)
	}
	
	if symbol.ID == 0 {
		id, err := result.LastInsertId()
		if err == nil {
			symbol.ID = int(id)
		}
	}
	
	return nil
}

// UpdateSymbol updates an existing symbol
func (mc *MySQLClient) UpdateSymbol(ctx context.Context, symbol *models.SymbolInfo) error {
	query := `
		UPDATE symbolsmap SET
			full_name = ?,
			instrument_type = ?,
			base_currency = ?,
			quote_currency = ?,
			is_active = ?,
			min_price_increment = ?,
			min_quantity_increment = ?,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`
	
	_, err := mc.db.ExecContext(ctx, query,
		symbol.FullName,
		symbol.InstrumentType,
		symbol.BaseCurrency,
		symbol.QuoteCurrency,
		symbol.IsActive,
		symbol.MinPriceIncrement,
		symbol.MinQuantityIncrement,
		symbol.ID,
	)
	
	if err != nil {
		return fmt.Errorf("failed to update symbol: %w", err)
	}
	
	return nil
}

// Session operations

// GetTradingSessions retrieves all active trading sessions
func (mc *MySQLClient) GetTradingSessions(ctx context.Context) ([]*models.TradingSession, error) {
	query := `
		SELECT id, name, start_time, end_time, timezone, days_active, is_active, created_at
		FROM trading_sessions
		WHERE is_active = 1
		ORDER BY start_time
	`
	
	rows, err := mc.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query trading sessions: %w", err)
	}
	defer rows.Close()
	
	var sessions []*models.TradingSession
	for rows.Next() {
		session := &models.TradingSession{}
		err := rows.Scan(
			&session.ID,
			&session.Name,
			&session.StartTime,
			&session.EndTime,
			&session.Timezone,
			&session.DaysActive,
			&session.IsActive,
			&session.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan trading session: %w", err)
		}
		sessions = append(sessions, session)
	}
	
	return sessions, rows.Err()
}

// Config operations

// GetSystemConfig retrieves system configuration by key
func (mc *MySQLClient) GetSystemConfig(ctx context.Context, key string) (*models.SystemConfig, error) {
	query := `
		SELECT id, config_key, config_value, description, created_at, updated_at
		FROM system_config
		WHERE config_key = ?
	`
	
	config := &models.SystemConfig{}
	err := mc.db.QueryRowContext(ctx, query, key).Scan(
		&config.ID,
		&config.ConfigKey,
		&config.ConfigValue,
		&config.Description,
		&config.CreatedAt,
		&config.UpdatedAt,
	)
	
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get system config: %w", err)
	}
	
	return config, nil
}

// GetAllSystemConfigs retrieves all system configurations
func (mc *MySQLClient) GetAllSystemConfigs(ctx context.Context) ([]*models.SystemConfig, error) {
	query := `
		SELECT id, config_key, config_value, description, created_at, updated_at
		FROM system_config
		ORDER BY config_key
	`
	
	rows, err := mc.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query system configs: %w", err)
	}
	defer rows.Close()
	
	var configs []*models.SystemConfig
	for rows.Next() {
		config := &models.SystemConfig{}
		err := rows.Scan(
			&config.ID,
			&config.ConfigKey,
			&config.ConfigValue,
			&config.Description,
			&config.CreatedAt,
			&config.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan system config: %w", err)
		}
		configs = append(configs, config)
	}
	
	return configs, rows.Err()
}

// SetSystemConfig sets a system configuration value
func (mc *MySQLClient) SetSystemConfig(ctx context.Context, key, value, description string) error {
	query := `
		INSERT INTO system_config (config_key, config_value, description)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE
			config_value = VALUES(config_value),
			description = VALUES(description),
			updated_at = CURRENT_TIMESTAMP
	`
	
	_, err := mc.db.ExecContext(ctx, query, key, value, description)
	if err != nil {
		return fmt.Errorf("failed to set system config: %w", err)
	}
	
	return nil
}

// Transaction support

// BeginTx starts a new transaction
func (mc *MySQLClient) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return mc.db.BeginTx(ctx, opts)
}

// ExecTx executes a function within a transaction
func (mc *MySQLClient) ExecTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := mc.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()
	
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}
		return err
	}
	
	return tx.Commit()
}