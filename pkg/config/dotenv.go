package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LoadDotEnv loads environment variables from .env file
func LoadDotEnv() error {
	// Try to find .env file in current directory and parent directories
	envFiles := []string{
		".env",
		"../.env",
		"../../.env",
		"../../../.env",
	}
	
	// Also check from the executable's directory
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		envFiles = append(envFiles, 
			filepath.Join(exeDir, ".env"),
			filepath.Join(exeDir, "..", ".env"),
		)
	}
	
	// Try each possible location
	for _, envFile := range envFiles {
		if err := loadEnvFile(envFile); err == nil {
			fmt.Printf("Loaded environment from: %s\n", envFile)
			return nil
		}
	}
	
	// If no .env file found, that's okay - use system env vars
	return nil
}

// loadEnvFile loads environment variables from a specific file
func loadEnvFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	
	scanner := bufio.NewScanner(file)
	lineNum := 0
	
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		
		// Handle export statements
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimPrefix(line, "export ")
		}
		
		// Parse key=value
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue // Skip malformed lines
		}
		
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		
		// Remove surrounding quotes if present
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') ||
			   (value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}
		
		// Only set if not already set (system env vars take precedence)
		if os.Getenv(key) == "" {
			if err := os.Setenv(key, value); err != nil {
				return fmt.Errorf("failed to set %s: %w", key, err)
			}
		}
	}
	
	return scanner.Err()
}

// MustLoadDotEnv loads .env file and panics on error
func MustLoadDotEnv() {
	if err := LoadDotEnv(); err != nil {
		panic(fmt.Sprintf("failed to load .env file: %v", err))
	}
}