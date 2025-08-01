package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/pkg/config"
	"net/http"
	"time"
)

// Logger wraps logrus logger with custom configuration
type Logger struct {
	*logrus.Logger
}

// New creates a new logger instance
func New(cfg *config.LoggingConfig) (*logrus.Logger, error) {
	logger := logrus.New()
	
	// Set log level
	level, err := logrus.ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level %s: %w", cfg.Level, err)
	}
	logger.SetLevel(level)
	
	// Set formatter
	switch cfg.Format {
	case "json":
		logger.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: "2006-01-02 15:04:05.000",
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyTime:  "timestamp",
				logrus.FieldKeyLevel: "level",
				logrus.FieldKeyMsg:   "message",
			},
		})
	default:
		logger.SetFormatter(&CustomTextFormatter{
			TextFormatter: logrus.TextFormatter{
				TimestampFormat: "2006-01-02 15:04:05",
				FullTimestamp:   true,
				ForceColors:     true,
			},
		})
	}
	
	// Set output
	output, err := getOutput(cfg.Output)
	if err != nil {
		return nil, fmt.Errorf("failed to set output: %w", err)
	}
	logger.SetOutput(output)
	
	// Set report caller
	logger.SetReportCaller(true)
	
	return logger, nil
}

// CustomTextFormatter is a custom text formatter for logrus
type CustomTextFormatter struct {
	logrus.TextFormatter
}

// Format renders a single log entry
func (f *CustomTextFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	// Custom formatting with colors and better structure
	levelColor := getColorByLevel(entry.Level)
	
	// Get caller information
	caller := ""
	if entry.HasCaller() {
		caller = fmt.Sprintf(" [%s]", formatCaller(entry.Caller))
	}
	
	// Format timestamp
	timestamp := entry.Time.Format(f.TimestampFormat)
	
	// Format fields
	fields := ""
	if len(entry.Data) > 0 {
		fields = " |"
		for k, v := range entry.Data {
			fields += fmt.Sprintf(" %s=%v", k, v)
		}
	}
	
	// Build the log line
	logLine := fmt.Sprintf("%s%s %s%s%s %s%s%s\n",
		"\033[90m", timestamp, "\033[0m", // Gray timestamp
		levelColor, strings.ToUpper(entry.Level.String()), "\033[0m", // Colored level
		caller,
		entry.Message,
		fields,
	)
	
	return []byte(logLine), nil
}

// getColorByLevel returns ANSI color code for log level
func getColorByLevel(level logrus.Level) string {
	switch level {
	case logrus.DebugLevel:
		return "\033[36m" // Cyan
	case logrus.InfoLevel:
		return "\033[32m" // Green
	case logrus.WarnLevel:
		return "\033[33m" // Yellow
	case logrus.ErrorLevel:
		return "\033[31m" // Red
	case logrus.FatalLevel, logrus.PanicLevel:
		return "\033[35m" // Magenta
	default:
		return "\033[0m" // Reset
	}
}

// formatCaller formats the caller information
func formatCaller(caller *runtime.Frame) string {
	// Get relative path
	_, file := filepath.Split(caller.File)
	
	// Extract function name
	funcName := caller.Function
	if idx := strings.LastIndex(funcName, "."); idx >= 0 {
		funcName = funcName[idx+1:]
	}
	
	return fmt.Sprintf("%s:%d %s", file, caller.Line, funcName)
}

// getOutput returns the appropriate output writer
func getOutput(output string) (io.Writer, error) {
	switch output {
	case "stdout":
		return os.Stdout, nil
	case "stderr":
		return os.Stderr, nil
	default:
		// Assume it's a file path
		file, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %s: %w", output, err)
		}
		return file, nil
	}
}

// WithComponent creates a logger with component field
func WithComponent(logger *logrus.Logger, component string) *logrus.Entry {
	return logger.WithField("component", component)
}

// WithSymbol creates a logger with symbol field
func WithSymbol(logger *logrus.Logger, symbol string) *logrus.Entry {
	return logger.WithField("symbol", symbol)
}

// WithError creates a logger with error field
func WithError(logger *logrus.Logger, err error) *logrus.Entry {
	return logger.WithError(err)
}

// Fields is a type alias for logrus.Fields
type Fields = logrus.Fields

// Middleware returns a logging middleware for HTTP handlers
func Middleware(logger *logrus.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			
			// Wrap response writer to capture status code
			wrapped := &responseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}
			
			// Process request
			next.ServeHTTP(wrapped, r)
			
			// Log request
			duration := time.Since(start)
			logger.WithFields(logrus.Fields{
				"method":     r.Method,
				"path":       r.URL.Path,
				"status":     wrapped.statusCode,
				"duration":   duration.Milliseconds(),
				"ip":         r.RemoteAddr,
				"user_agent": r.UserAgent(),
			}).Info("HTTP request")
		})
	}
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}