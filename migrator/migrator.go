package migrator

import (
	"database/sql"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/knadh/goyesql"
	_ "github.com/lib/pq"
)

type Migrator struct {
	log                  *slog.Logger
	sql                  goyesql.Queries
	schemaFileName       string
	migrationsFolderName string

	DBC                  *sql.DB
	SQLFolder            string
	SchemaFileName       string
	MigrationsFolderName string
}

func NewMigratorWithPostgresURL(dbURL string, sf string) (*Migrator, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	return NewMigratorWithSqlClient(db, sf)
}

func NewMigratorWithSqlClient(s *sql.DB, sf string) (*Migrator, error) {
	schemaFileName := "schema.sql"
	migrationsFolderName := "migrations"

	if !checkFolders(sf, schemaFileName, migrationsFolderName) {
		return nil, fmt.Errorf("folders and files are not organised according to the documentation")
	}

	schema, err := goyesql.ParseFile(sf + schemaFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema file: %w", err)
	}

	return &Migrator{
		SQLFolder:            sf,
		DBC:                  s,
		SchemaFileName:       schemaFileName,
		MigrationsFolderName: migrationsFolderName,

		sql:                  schema,
		schemaFileName:       schemaFileName,
		migrationsFolderName: migrationsFolderName,
	}, nil
}

func (m *Migrator) IsInitialised() (bool, error) {
	var count int
	err := m.DBC.QueryRow(`
        SELECT COUNT(tablename)
        FROM pg_tables
        WHERE tablename = 'migrations';`).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to scan result: %w", err)
	}
	return count == 1, nil
}

func (m *Migrator) CountMigrations() (int, error) {
	files, err := listFilesFilter(m.SQLFolder+m.migrationsFolderName, "*.sql")
	if err != nil {
		return 0, fmt.Errorf("error opening migrations directory: %w", err)
	}
	return len(files), nil
}

func (m *Migrator) ApplySchemaUp() error {
	initialised, err := m.IsInitialised()
	if err != nil {
		return err
	}
	if initialised {
		return ErrSchemaAlreadyInitialised
	}

	sq, ok := m.sql["schema_up"]
	if !ok {
		return fmt.Errorf("schema_up not found")
	}

	tx, err := m.DBC.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(sq.Query); err != nil {
		return fmt.Errorf("failed to execute schema_up: %w", err)
	}

	if _, err := tx.Exec(`
        CREATE TABLE IF NOT EXISTS migrations (
            id SERIAL PRIMARY KEY,
            version INTEGER NOT NULL
        );`); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	count, err := m.CountMigrations()
	if err != nil {
		return err
	}

	if _, err := tx.Exec(`
        INSERT INTO migrations (id, version)
        VALUES (1, $1)
        ON CONFLICT (id)
        DO UPDATE SET version = EXCLUDED.version;
    `, count); err != nil {
		return fmt.Errorf("failed to insert migration version: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (m *Migrator) ApplySchemaDown() error {
	sq, ok := m.sql["schema_down"]
	if !ok {
		return fmt.Errorf("schema_down not found")
	}

	tx, err := m.DBC.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(sq.Query); err != nil {
		return fmt.Errorf("failed to execute schema_down: %w", err)
	}

	if _, err := tx.Exec(`DROP TABLE IF EXISTS migrations;`); err != nil {
		return fmt.Errorf("failed to drop migrations table: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (m *Migrator) ApplyMigration() error {
	var version int
	err := m.DBC.QueryRow("SELECT version FROM migrations WHERE id = 1").Scan(&version)
	if err != nil {
		return fmt.Errorf("error querying migrations table: %w", err)
	}

	files, err := os.ReadDir(m.SQLFolder + m.migrationsFolderName)
	if err != nil {
		return fmt.Errorf("error opening migrations directory: %w", err)
	}

	sqlFiles := []fs.DirEntry{}
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".sql") {
			sqlFiles = append(sqlFiles, file)
		}
	}

	if len(sqlFiles) == 0 {
		return nil // No migrations to apply
	}

	var toApply []int
	for _, sqlFile := range sqlFiles {
		split := strings.Split(sqlFile.Name(), ".")
		if len(split) != 2 {
			continue
		}
		level, err := strconv.Atoi(split[0])
		if err != nil {
			return fmt.Errorf("could not parse migration filename: %w", err)
		}
		if level > version {
			toApply = append(toApply, level)
		}
	}

	if len(toApply) == 0 {
		return nil // No new migrations to apply
	}

	slices.Sort(toApply)

	for _, l := range toApply {
		migration, err := os.ReadFile(fmt.Sprintf("%s%s%d.sql", m.SQLFolder, m.migrationsFolderName, l))
		if err != nil {
			return fmt.Errorf("could not read migration file %d: %w", l, err)
		}

		tx, err := m.DBC.Begin()
		if err != nil {
			return fmt.Errorf("error beginning transaction: %w", err)
		}

		if _, err := tx.Exec(string(migration)); err != nil {
			tx.Rollback()
			return fmt.Errorf("error executing migration %d: %w", l, err)
		}

		if _, err := tx.Exec(`
            INSERT INTO migrations (id, version)
            VALUES (1, $1)
            ON CONFLICT (id)
            DO UPDATE SET version = EXCLUDED.version;
        `, l); err != nil {
			tx.Rollback()
			return fmt.Errorf("error updating migration version: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		fmt.Printf("Applied migration: %d\n", l)
	}

	return nil
}

func (m *Migrator) RunSQLScript(name string, args ...any) error {
	sq, ok := m.sql[name]
	if !ok {
		return fmt.Errorf("SQL script '%s' not found", name)
	}

	if len(args) == 0 {
		_, err := m.DBC.Exec(sq.Query)
		if err != nil {
			return fmt.Errorf("exec failed on script %s: %w", name, err)
		}
	} else {
		_, err := m.DBC.Exec(sq.Query, args...)
		if err != nil {
			return fmt.Errorf("exec failed on script %s: %w", name, err)
		}
	}

	return nil
}

func checkFolders(sqlFolder, schemaFileName, migrationsFolderName string) bool {
	sqlFolderInfo, err := os.Stat(sqlFolder)
	if os.IsNotExist(err) {
		fmt.Println("SQL folder provided does not exist")
		return false
	}

	if !sqlFolderInfo.IsDir() {
		fmt.Println("SQL folder provided is not a directory")
		return false
	}

	schemaFileInfo, err := os.Stat(sqlFolder + schemaFileName)
	if os.IsNotExist(err) {
		fmt.Println("Schema file (schema.sql) inside SQL folder provided does not exist")
		return false
	}

	if schemaFileInfo.IsDir() {
		fmt.Println("Schema directory inside SQL folder is not supported")
		return false
	}

	migrationsFolderInfo, err := os.Stat(sqlFolder + migrationsFolderName)
	if os.IsNotExist(err) {
		fmt.Println("Migrations (migrations) directory inside SQL folder does not exist")
		return false
	}

	if !migrationsFolderInfo.IsDir() {
		fmt.Println("Migrations (migrations) directory inside SQL folder is not a directory")
		return false
	}

	return true
}

func listFilesFilter(root, pattern string) ([]string, error) {
	var matches []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if matched, err := filepath.Match(pattern, filepath.Base(path)); err != nil {
			return err
		} else if matched {
			matches = append(matches, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return matches, nil
}
