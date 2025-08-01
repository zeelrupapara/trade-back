package commands

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
	"github.com/trade-back/pkg/config"
)

var (
	migrationPath string
	dryRun        bool
	rollback      bool
)

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Database migration management",
	Long: `Manage database migrations for the trading backend.

This command handles database schema migrations including:
• Running pending migrations
• Rolling back migrations  
• Checking migration status
• Creating migration files

Examples:
  trade-back migrate up                    # Run all pending migrations
  trade-back migrate down                  # Rollback last migration
  trade-back migrate status                # Show migration status
  trade-back migrate create add_new_table  # Create new migration file`,
}

// migrateUpCmd runs pending migrations
var migrateUpCmd = &cobra.Command{
	Use:   "up",
	Short: "Run pending migrations",
	Long:  "Execute all pending database migrations",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runMigrations(false)
	},
}

// migrateDownCmd rolls back migrations
var migrateDownCmd = &cobra.Command{
	Use:   "down",
	Short: "Rollback migrations",
	Long:  "Rollback the last applied migration",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runMigrations(true)
	},
}

// migrateStatusCmd shows migration status
var migrateStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show migration status",
	Long:  "Display the status of all migrations",
	RunE: func(cmd *cobra.Command, args []string) error {
		return showMigrationStatus()
	},
}

// migrateCreateCmd creates a new migration file
var migrateCreateCmd = &cobra.Command{
	Use:   "create [name]",
	Short: "Create a new migration file",
	Long:  "Create a new migration file with the given name",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return createMigration(args[0])
	},
}

func init() {
	rootCmd.AddCommand(migrateCmd)
	
	// Add subcommands
	migrateCmd.AddCommand(migrateUpCmd)
	migrateCmd.AddCommand(migrateDownCmd)
	migrateCmd.AddCommand(migrateStatusCmd)
	migrateCmd.AddCommand(migrateCreateCmd)

	// Flags
	migrateCmd.PersistentFlags().StringVarP(&migrationPath, "path", "p", "./migrations", "Path to migration files")
	migrateCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "Show what would be executed without running")
	
	migrateDownCmd.Flags().BoolVar(&rollback, "rollback", false, "Confirm rollback operation")
}

type Migration struct {
	Version   string
	Name      string
	UpSQL     string
	DownSQL   string
	Applied   bool
	AppliedAt *time.Time
}

func runMigrations(down bool) error {
	db, err := connectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Ensure migrations table exists
	if err := createMigrationsTable(db); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Get all migrations
	migrations, err := loadMigrations()
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}

	// Get applied migrations
	appliedMigrations, err := getAppliedMigrations(db)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}

	// Mark applied migrations
	for i := range migrations {
		if _, exists := appliedMigrations[migrations[i].Version]; exists {
			migrations[i].Applied = true
			if appliedMigrations[migrations[i].Version] != nil {
				migrations[i].AppliedAt = appliedMigrations[migrations[i].Version]
			}
		}
	}

	if down {
		return rollbackMigration(db, migrations)
	}
	
	return applyMigrations(db, migrations)
}

func applyMigrations(db *sql.DB, migrations []Migration) error {
	pendingCount := 0
	for _, migration := range migrations {
		if !migration.Applied {
			pendingCount++
		}
	}

	if pendingCount == 0 {
		fmt.Println("✅ No pending migrations")
		return nil
	}

	fmt.Printf("Found %d pending migration(s)\n\n", pendingCount)

	for _, migration := range migrations {
		if migration.Applied {
			continue
		}

		fmt.Printf("Applying migration: %s - %s\n", migration.Version, migration.Name)

		if dryRun {
			fmt.Printf("  [DRY RUN] Would execute:\n%s\n\n", migration.UpSQL)
			continue
		}

		// Start transaction
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}

		// Execute migration
		if _, err := tx.Exec(migration.UpSQL); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute migration %s: %w", migration.Version, err)
		}

		// Record migration
		if _, err := tx.Exec(
			"INSERT INTO migrations (version, name, applied_at) VALUES (?, ?, ?)",
			migration.Version, migration.Name, time.Now(),
		); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to record migration %s: %w", migration.Version, err)
		}

		// Commit transaction
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit migration %s: %w", migration.Version, err)
		}

		fmt.Printf("  ✅ Applied successfully\n\n")
	}

	fmt.Println("🎉 All migrations applied successfully!")
	return nil
}

func rollbackMigration(db *sql.DB, migrations []Migration) error {
	// Find last applied migration
	var lastMigration *Migration
	for i := len(migrations) - 1; i >= 0; i-- {
		if migrations[i].Applied {
			lastMigration = &migrations[i]
			break
		}
	}

	if lastMigration == nil {
		fmt.Println("❌ No migrations to rollback")
		return nil
	}

	fmt.Printf("Rolling back migration: %s - %s\n", lastMigration.Version, lastMigration.Name)

	if !rollback && !dryRun {
		fmt.Println("❌ Rollback requires --rollback flag for confirmation")
		return nil
	}

	if dryRun {
		fmt.Printf("  [DRY RUN] Would execute:\n%s\n", lastMigration.DownSQL)
		return nil
	}

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	// Execute rollback
	if _, err := tx.Exec(lastMigration.DownSQL); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to rollback migration %s: %w", lastMigration.Version, err)
	}

	// Remove migration record
	if _, err := tx.Exec("DELETE FROM migrations WHERE version = ?", lastMigration.Version); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to remove migration record %s: %w", lastMigration.Version, err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit rollback %s: %w", lastMigration.Version, err)
	}

	fmt.Printf("  ✅ Rolled back successfully\n")
	return nil
}

func showMigrationStatus() error {
	db, err := connectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Ensure migrations table exists
	if err := createMigrationsTable(db); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Get all migrations
	migrations, err := loadMigrations()
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}

	// Get applied migrations
	appliedMigrations, err := getAppliedMigrations(db)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}

	// Mark applied migrations
	for i := range migrations {
		if appliedAt, exists := appliedMigrations[migrations[i].Version]; exists {
			migrations[i].Applied = true
			migrations[i].AppliedAt = appliedAt
		}
	}

	fmt.Println("Migration Status:")
	fmt.Println("=================")
	fmt.Printf("%-20s %-30s %-10s %s\n", "Version", "Name", "Status", "Applied At")
	fmt.Println(strings.Repeat("-", 80))

	for _, migration := range migrations {
		status := "❌ Pending"
		appliedAt := "-"
		
		if migration.Applied {
			status = "✅ Applied"
			if migration.AppliedAt != nil {
				appliedAt = migration.AppliedAt.Format("2006-01-02 15:04:05")
			}
		}

		fmt.Printf("%-20s %-30s %-10s %s\n", 
			migration.Version, 
			migration.Name, 
			status, 
			appliedAt)
	}

	return nil
}

func createMigration(name string) error {
	// Create migrations directory if it doesn't exist
	if err := os.MkdirAll(migrationPath, 0755); err != nil {
		return fmt.Errorf("failed to create migrations directory: %w", err)
	}

	// Generate version (timestamp)
	version := time.Now().Format("20060102150405")
	
	// Clean name (replace spaces with underscores, lowercase)
	cleanName := strings.ToLower(strings.ReplaceAll(name, " ", "_"))
	
	filename := fmt.Sprintf("%s_%s.sql", version, cleanName)
	filepath := filepath.Join(migrationPath, filename)

	template := fmt.Sprintf(`-- Migration: %s
-- Created: %s

-- +migrate Up
-- Add your UP migration SQL here


-- +migrate Down  
-- Add your DOWN migration SQL here

`, name, time.Now().Format("2006-01-02 15:04:05"))

	if err := ioutil.WriteFile(filepath, []byte(template), 0644); err != nil {
		return fmt.Errorf("failed to create migration file: %w", err)
	}

	fmt.Printf("✅ Created migration file: %s\n", filepath)
	return nil
}

func connectDB() (*sql.DB, error) {
	// Load .env file first
	config.LoadDotEnv()
	
	host := getEnv("MYSQL_HOST", "localhost")
	port := getEnvInt("MYSQL_PORT", 3306)
	database := getEnv("MYSQL_DATABASE", "trading")
	username := getEnv("MYSQL_USER", "trading")
	password := getEnv("MYSQL_PASSWORD", "trading123")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true",
		username, password, host, port, database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}

	return db, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func createMigrationsTable(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS migrations (
			version VARCHAR(14) PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		) ENGINE=InnoDB
	`
	_, err := db.Exec(query)
	return err
}

func loadMigrations() ([]Migration, error) {
	files, err := ioutil.ReadDir(migrationPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []Migration{}, nil
		}
		return nil, err
	}

	var migrations []Migration
	
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}

		migration, err := parseMigrationFile(filepath.Join(migrationPath, file.Name()))
		if err != nil {
			return nil, fmt.Errorf("failed to parse migration %s: %w", file.Name(), err)
		}

		migrations = append(migrations, migration)
	}

	// Sort by version
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

func parseMigrationFile(filepath string) (Migration, error) {
	content, err := ioutil.ReadFile(filepath)
	if err != nil {
		return Migration{}, err
	}

	filename := filepath[strings.LastIndex(filepath, "/")+1:]
	parts := strings.SplitN(filename, "_", 2)
	if len(parts) != 2 {
		return Migration{}, fmt.Errorf("invalid migration filename format: %s", filename)
	}

	version := parts[0]
	name := strings.TrimSuffix(parts[1], ".sql")

	// Parse UP and DOWN sections
	lines := strings.Split(string(content), "\n")
	var upSQL, downSQL strings.Builder
	var currentSection string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		
		if strings.HasPrefix(trimmed, "-- +migrate Up") {
			currentSection = "up"
			continue
		} else if strings.HasPrefix(trimmed, "-- +migrate Down") {
			currentSection = "down"
			continue
		}

		// Skip comments and empty lines
		if strings.HasPrefix(trimmed, "--") || trimmed == "" {
			continue
		}

		switch currentSection {
		case "up":
			upSQL.WriteString(line + "\n")
		case "down":
			downSQL.WriteString(line + "\n")
		}
	}

	return Migration{
		Version: version,
		Name:    name,
		UpSQL:   strings.TrimSpace(upSQL.String()),
		DownSQL: strings.TrimSpace(downSQL.String()),
	}, nil
}

func getAppliedMigrations(db *sql.DB) (map[string]*time.Time, error) {
	rows, err := db.Query("SELECT version, applied_at FROM migrations ORDER BY version")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[string]*time.Time)
	
	for rows.Next() {
		var version string
		var appliedAt time.Time
		
		if err := rows.Scan(&version, &appliedAt); err != nil {
			return nil, err
		}
		
		applied[version] = &appliedAt
	}

	return applied, rows.Err()
}