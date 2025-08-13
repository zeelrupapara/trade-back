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
	
	// logger.WithFields(logrus.Fields{
	// 	"host": cfg.Host,
	// 	"port": cfg.Port,
	// 	"database": cfg.Database,
	// 	"username": cfg.Username,
	// }).Info("Connecting to MySQL")
	
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
	
	rows, err := mc.db.QueryContext(ctx, query)
	if err != nil {
		mc.logger.WithError(err).Error("Failed to query symbols")
		return nil, fmt.Errorf("failed to query symbols: %w", err)
	}
	defer rows.Close()
	
	
	var symbols []*models.SymbolInfo
	rowCount := 0
	for rows.Next() {
		rowCount++
		symbol := &models.SymbolInfo{}
		var minPriceIncrement, minQuantityIncrement sql.NullFloat64
		err := rows.Scan(
			&symbol.ID,
			&symbol.Exchange,
			&symbol.Symbol,
			&symbol.FullName,
			&symbol.InstrumentType,
			&symbol.BaseCurrency,
			&symbol.QuoteCurrency,
			&symbol.IsActive,
			&minPriceIncrement,
			&minQuantityIncrement,
			&symbol.CreatedAt,
			&symbol.UpdatedAt,
		)
		if err != nil {
			mc.logger.WithError(err).WithField("row", rowCount).Error("Failed to scan symbol")
			return nil, fmt.Errorf("failed to scan symbol at row %d: %w", rowCount, err)
		}
		
		// Handle NULL values
		if minPriceIncrement.Valid {
			symbol.MinPriceIncrement = minPriceIncrement.Float64
		}
		if minQuantityIncrement.Valid {
			symbol.MinQuantityIncrement = minQuantityIncrement.Float64
		}
		
		symbols = append(symbols, symbol)
		
	}
	
	if err := rows.Err(); err != nil {
		mc.logger.WithError(err).Error("Error iterating rows")
		return nil, err
	}
	
	return symbols, nil
}

// GetMarketWatchSymbols retrieves symbols from market watch for a given session
func (mc *MySQLClient) GetMarketWatchSymbols(ctx context.Context, sessionToken string) ([]string, error) {
	query := `
		SELECT s.symbol
		FROM market_watch mw
		JOIN symbolsmap s ON mw.symbol_id = s.id
		WHERE mw.session_token = ? AND s.is_active = true
		ORDER BY mw.added_at DESC
	`
	
	rows, err := mc.db.QueryContext(ctx, query, sessionToken)
	if err != nil {
		return nil, fmt.Errorf("failed to query market watch symbols: %w", err)
	}
	defer rows.Close()
	
	var symbols []string
	for rows.Next() {
		var symbol string
		if err := rows.Scan(&symbol); err != nil {
			return nil, fmt.Errorf("failed to scan symbol: %w", err)
		}
		symbols = append(symbols, symbol)
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}
	
	
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

// SymbolExists checks if a symbol exists in the database
func (mc *MySQLClient) SymbolExists(ctx context.Context, exchange, symbol string) (bool, error) {
	query := "SELECT COUNT(*) FROM symbolsmap WHERE exchange = ? AND symbol = ?"
	
	var count int
	err := mc.db.QueryRowContext(ctx, query, exchange, symbol).Scan(&count)
	if err != nil {
		return false, err
	}
	
	return count > 0, nil
}

// GetSymbolByName gets a symbol by name from any exchange
func (mc *MySQLClient) GetSymbolByName(ctx context.Context, symbol string) (*models.SymbolInfo, error) {
	query := `
		SELECT id, exchange, symbol, full_name, instrument_type, 
		       base_currency, quote_currency, is_active,
		       min_price_increment, min_quantity_increment, 
		       created_at, updated_at
		FROM symbolsmap 
		WHERE symbol = ? AND is_active = 1 
		ORDER BY CASE WHEN exchange = 'binance' THEN 1 ELSE 2 END
		LIMIT 1
	`
	
	var symbolInfo models.SymbolInfo
	err := mc.db.QueryRowContext(ctx, query, symbol).Scan(
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
	
	if err != nil {
		return nil, err
	}
	
	return &symbolInfo, nil
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


// GetMarketWatchWithSyncStatus retrieves market watch symbols with sync status
func (mc *MySQLClient) GetMarketWatchWithSyncStatus(ctx context.Context, sessionToken string) ([]map[string]interface{}, error) {
	query := `
		SELECT s.symbol, mw.sync_status, mw.sync_progress, mw.total_bars,
		       mw.sync_started_at, mw.sync_completed_at
		FROM market_watch mw
		JOIN symbolsmap s ON mw.symbol_id = s.id
		WHERE mw.session_token = ? AND s.is_active = true
		ORDER BY mw.added_at DESC
	`
	
	rows, err := mc.db.QueryContext(ctx, query, sessionToken)
	if err != nil {
		mc.logger.WithError(err).WithField("token", sessionToken).Error("Failed to query market watch")
		return nil, fmt.Errorf("failed to query market watch with status: %w", err)
	}
	defer rows.Close()
	
	var results []map[string]interface{}
	// mc.logger.WithField("token", sessionToken).Info("Querying market watch symbols")
	for rows.Next() {
		var symbol string
		var syncStatus sql.NullString
		var syncProgress, totalBars sql.NullInt64
		var syncStartedAt, syncCompletedAt sql.NullTime
		
		if err := rows.Scan(&symbol, &syncStatus, &syncProgress, &totalBars, 
			&syncStartedAt, &syncCompletedAt); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		
		result := map[string]interface{}{
			"symbol": symbol,
		}
		
		// Add non-null values
		if syncStatus.Valid {
			result["sync_status"] = syncStatus.String
		} else {
			result["sync_status"] = "pending"
		}
		
		if syncProgress.Valid {
			result["sync_progress"] = int(syncProgress.Int64)
		} else {
			result["sync_progress"] = 0
		}
		
		if totalBars.Valid {
			result["total_bars"] = int(totalBars.Int64)
		} else {
			result["total_bars"] = 0
		}
		
		if syncStartedAt.Valid {
			result["sync_started_at"] = syncStartedAt.Time
		}
		if syncCompletedAt.Valid {
			result["sync_completed_at"] = syncCompletedAt.Time
		}
		
		results = append(results, result)
	}
	
	// mc.logger.WithFields(logrus.Fields{
	// 	"token": sessionToken,
	// 	"count": len(results),
	// }).Info("Market watch query complete")
	
	return results, nil
}

// AddToMarketWatch adds a symbol to user's market watch
func (mc *MySQLClient) AddToMarketWatch(ctx context.Context, sessionToken, symbol string) error {
	// First get the symbol ID
	var symbolID int
	query := "SELECT id FROM symbolsmap WHERE symbol = ? AND is_active = true"
	err := mc.db.QueryRowContext(ctx, query, symbol).Scan(&symbolID)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("symbol not found: %s", symbol)
		}
		return fmt.Errorf("failed to get symbol ID: %w", err)
	}
	
	// Insert into market_watch table
	insertQuery := `
		INSERT INTO market_watch (symbol_id, session_token, sync_status)
		VALUES (?, ?, 'pending')
		ON DUPLICATE KEY UPDATE added_at = CURRENT_TIMESTAMP
	`
	
	_, err = mc.db.ExecContext(ctx, insertQuery, symbolID, sessionToken)
	if err != nil {
		return fmt.Errorf("failed to add to market watch: %w", err)
	}
	
	return nil
}

// RemoveFromMarketWatch removes a symbol from user's market watch
func (mc *MySQLClient) RemoveFromMarketWatch(ctx context.Context, sessionToken, symbol string) error {
	query := `
		DELETE mw FROM market_watch mw
		JOIN symbolsmap s ON mw.symbol_id = s.id
		WHERE mw.session_token = ? AND s.symbol = ?
	`
	
	result, err := mc.db.ExecContext(ctx, query, sessionToken, symbol)
	if err != nil {
		return fmt.Errorf("failed to remove from market watch: %w", err)
	}
	
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	
	if rows == 0 {
		return fmt.Errorf("symbol not found in market watch")
	}
	
	return nil
}

// GetPendingSyncSymbols gets all market watch symbols that need syncing
func (mc *MySQLClient) GetPendingSyncSymbols(ctx context.Context) ([]map[string]interface{}, error) {
	query := `
		SELECT s.symbol, mw.session_token, mw.sync_status, mw.added_at
		FROM market_watch mw
		JOIN symbolsmap s ON mw.symbol_id = s.id
		WHERE (mw.sync_status IN ('pending', 'failed') 
		   OR mw.sync_status IS NULL)
		   AND s.is_active = true
		ORDER BY mw.added_at ASC
	`
	
	rows, err := mc.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending sync symbols: %w", err)
	}
	defer rows.Close()
	
	var results []map[string]interface{}
	for rows.Next() {
		var symbol, sessionToken string
		var syncStatus sql.NullString
		var addedAt time.Time
		
		if err := rows.Scan(&symbol, &sessionToken, &syncStatus, &addedAt); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		
		result := map[string]interface{}{
			"symbol": symbol,
			"session_token": sessionToken,
		}
		
		if syncStatus.Valid {
			result["sync_status"] = syncStatus.String
		} else {
			result["sync_status"] = "pending"
		}
		
		results = append(results, result)
	}
	
	return results, nil
}

// GetSyncStatus gets the current sync status for a symbol
func (mc *MySQLClient) GetSyncStatus(ctx context.Context, symbol string) (*models.SyncStatus, error) {
	query := `
		SELECT 
			s.symbol,
			COALESCE(ss.status, 'pending') as status,
			COALESCE(ss.progress, 0) as progress,
			COALESCE(ss.total_bars, 0) as total_bars,
			COALESCE(ss.error_message, '') as error,
			COALESCE(ss.updated_at, NOW()) as updated_at
		FROM symbolsmap s
		LEFT JOIN sync_status ss ON s.id = ss.symbol_id
		WHERE s.symbol = ?
		LIMIT 1
	`
	
	var status models.SyncStatus
	var errorMsg sql.NullString
	
	err := mc.db.QueryRowContext(ctx, query, symbol).Scan(
		&status.Symbol,
		&status.Status,
		&status.Progress,
		&status.TotalBars,
		&errorMsg,
		&status.UpdatedAt,
	)
	
	if err != nil {
		if err == sql.ErrNoRows {
			// Symbol not found, return pending status
			return &models.SyncStatus{
				Symbol:    symbol,
				Status:    "pending",
				Progress:  0,
				TotalBars: 0,
				UpdatedAt: time.Now(),
			}, nil
		}
		return nil, fmt.Errorf("failed to get sync status: %w", err)
	}
	
	if errorMsg.Valid {
		status.Error = errorMsg.String
	}
	
	return &status, nil
}

// UpdateSyncStatus updates the sync status for a symbol
func (mc *MySQLClient) UpdateSyncStatus(ctx context.Context, symbol string, status string, progress int, totalBars int, errorMsg string) error {
	// First get symbol ID
	var symbolID int
	err := mc.db.QueryRowContext(ctx, "SELECT id FROM symbolsmap WHERE symbol = ?", symbol).Scan(&symbolID)
	if err != nil {
		return fmt.Errorf("failed to get symbol ID: %w", err)
	}
	
	// Update or insert sync status
	query := `
		INSERT INTO sync_status (symbol_id, status, progress, total_bars, error_message, updated_at)
		VALUES (?, ?, ?, ?, ?, NOW())
		ON DUPLICATE KEY UPDATE
			status = VALUES(status),
			progress = VALUES(progress),
			total_bars = VALUES(total_bars),
			error_message = VALUES(error_message),
			updated_at = NOW()
	`
	
	var errorPtr *string
	if errorMsg != "" {
		errorPtr = &errorMsg
	}
	
	_, err = mc.db.ExecContext(ctx, query, symbolID, status, progress, totalBars, errorPtr)
	if err != nil {
		return fmt.Errorf("failed to update sync status: %w", err)
	}
	
	// Also update market_watch table sync status
	updateMW := `
		UPDATE market_watch mw
		SET sync_status = ?, sync_progress = ?
		WHERE symbol_id = ?
	`
	_, err = mc.db.ExecContext(ctx, updateMW, status, progress, symbolID)
	if err != nil {
		// Log but don't fail
		mc.logger.WithError(err).Warn("Failed to update market_watch sync status")
	}
	
	return nil
}

// Asset Extremes operations

// StoreAssetExtreme stores or updates asset extreme values
func (mc *MySQLClient) StoreAssetExtreme(ctx context.Context, extreme *models.AssetExtreme) error {
	// Get symbol ID from symbol name
	var symbolID int
	err := mc.db.QueryRowContext(ctx, 
		"SELECT id FROM symbolsmap WHERE symbol = ? LIMIT 1", 
		extreme.Symbol).Scan(&symbolID)
	if err != nil {
		return fmt.Errorf("failed to get symbol ID for %s: %w", extreme.Symbol, err)
	}

	// Determine exchange (default to binance if not specified)
	exchange := "binance"
	if extreme.AssetClass == models.AssetClassForex {
		exchange = "oanda"
	}

	query := `
		INSERT INTO asset_extremes (
			symbol_id, exchange, ath, atl, ath_date, atl_date,
			week_52_high, week_52_low, data_source, last_calculated
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
		ON DUPLICATE KEY UPDATE
			ath = VALUES(ath),
			atl = VALUES(atl),
			ath_date = VALUES(ath_date),
			atl_date = VALUES(atl_date),
			week_52_high = VALUES(week_52_high),
			week_52_low = VALUES(week_52_low),
			data_source = VALUES(data_source),
			last_calculated = NOW(),
			updated_at = NOW()
	`

	// Parse dates
	var athDate, atlDate *time.Time
	if extreme.ATHDate != "" {
		t, err := time.Parse("2006-01-02", extreme.ATHDate)
		if err == nil {
			athDate = &t
		}
	}
	if extreme.ATLDate != "" {
		t, err := time.Parse("2006-01-02", extreme.ATLDate)
		if err == nil {
			atlDate = &t
		}
	}

	_, err = mc.db.ExecContext(ctx, query,
		symbolID, exchange, extreme.ATH, extreme.ATL, athDate, atlDate,
		extreme.Week52High, extreme.Week52Low, extreme.DataSource)
	
	if err != nil {
		return fmt.Errorf("failed to store asset extreme: %w", err)
	}

	return nil
}

// GetAssetExtreme retrieves asset extreme values
func (mc *MySQLClient) GetAssetExtreme(ctx context.Context, symbol string) (*models.AssetExtreme, error) {
	query := `
		SELECT 
			ae.ath, ae.atl, ae.ath_date, ae.atl_date,
			ae.week_52_high, ae.week_52_low, ae.month_high, ae.month_low,
			ae.day_high, ae.day_low, ae.data_source, ae.confidence_score,
			ae.last_calculated, sm.symbol, ae.exchange
		FROM asset_extremes ae
		JOIN symbolsmap sm ON ae.symbol_id = sm.id
		WHERE sm.symbol = ?
		ORDER BY ae.last_calculated DESC
		LIMIT 1
	`

	var extreme models.AssetExtreme
	var athDate, atlDate sql.NullTime
	var monthHigh, monthLow, dayHigh, dayLow sql.NullFloat64
	var confidenceScore sql.NullFloat64
	var lastCalculated time.Time
	var exchange string

	err := mc.db.QueryRowContext(ctx, query, symbol).Scan(
		&extreme.ATH, &extreme.ATL, &athDate, &atlDate,
		&extreme.Week52High, &extreme.Week52Low, &monthHigh, &monthLow,
		&dayHigh, &dayLow, &extreme.DataSource, &confidenceScore,
		&lastCalculated, &extreme.Symbol, &exchange)
	
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get asset extreme: %w", err)
	}

	// Set dates
	if athDate.Valid {
		extreme.ATHDate = athDate.Time.Format("2006-01-02")
	}
	if atlDate.Valid {
		extreme.ATLDate = atlDate.Time.Format("2006-01-02")
	}

	// Set asset class based on exchange
	if exchange == "oanda" {
		extreme.AssetClass = models.AssetClassForex
	} else {
		extreme.AssetClass = models.AssetClassCrypto
	}

	extreme.LastUpdated = lastCalculated.Unix()

	return &extreme, nil
}

// UpdateAssetExtremePeriods updates period-based extremes (day, month)
func (mc *MySQLClient) UpdateAssetExtremePeriods(ctx context.Context, symbolID int, exchange string, 
	dayHigh, dayLow, monthHigh, monthLow float64) error {
	
	query := `
		UPDATE asset_extremes 
		SET day_high = ?, day_low = ?, month_high = ?, month_low = ?,
		    updated_at = NOW()
		WHERE symbol_id = ? AND exchange = ?
	`

	_, err := mc.db.ExecContext(ctx, query, 
		dayHigh, dayLow, monthHigh, monthLow, symbolID, exchange)
	
	if err != nil {
		return fmt.Errorf("failed to update asset extreme periods: %w", err)
	}

	return nil
}