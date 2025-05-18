package logger

import (
	"database/sql"
	"fmt"
	"os"

	_ "modernc.org/sqlite" // pure-Go SQLite driver
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// InitLogger initializes a zap.Logger that writes logs to both console and a SQLite logging database.
func InitLogger(dbPath string) (*zap.Logger, error) {
	// Open or create the SQLite database using pure-Go driver
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open logging database: %w", err)
	}

	// Create logs table if it doesn't exist
	createTable := `
	CREATE TABLE IF NOT EXISTS logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		entry TEXT
	);
	`
	if _, err := db.Exec(createTable); err != nil {
		return nil, fmt.Errorf("create logs table: %w", err)
	}

	// Console encoder
	consoleEncCfg := zap.NewDevelopmentEncoderConfig()
	consoleEncoder := zapcore.NewConsoleEncoder(consoleEncCfg)

	// JSON encoder for DB storage
	jsonEncCfg := zap.NewProductionEncoderConfig()
	jsonEncoder := zapcore.NewJSONEncoder(jsonEncCfg)

	// WriteSyncers
	wsConsole := zapcore.AddSync(os.Stdout)
	wsDB := zapcore.AddSync(&dbWriteSyncer{db: db})

	// Combine cores
	core := zapcore.NewTee(
		zapcore.NewCore(consoleEncoder, wsConsole, zap.DebugLevel),
		zapcore.NewCore(jsonEncoder, wsDB, zap.InfoLevel),
	)

	// Build logger
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	return logger, nil
}

// dbWriteSyncer implements zapcore.WriteSyncer for SQLite
type dbWriteSyncer struct {
	db *sql.DB
}

// Write inserts the log entry into the logs table
func (w *dbWriteSyncer) Write(p []byte) (n int, err error) {
	stmt := `INSERT INTO logs(entry) VALUES(?)` 
	if _, err := w.db.Exec(stmt, string(p)); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Sync is a no-op for SQLite
func (w *dbWriteSyncer) Sync() error {
	return nil
}
