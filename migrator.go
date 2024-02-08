package migrator

import (
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/knadh/goyesql"
	_ "github.com/lib/pq"
)

const (
	migrationsFolderName = "migrations/"
	schemaFileName       = "schema.sql"
)

type Migrator struct {
	sql       goyesql.Queries
	DBC       *sqlx.DB
	SQLFolder string
}

// NewMigratorWithPostgresURL - initialises migrator and sqlx client with postgres db url.
// Panics on error.
func NewMigratorWithPostgresURL(dbURL string, sf string) *Migrator {
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		panic(err)
	}

	return NewMigratorWithSqlxClient(db, sf)
}

// NewMigratorWithSqlxClient - initialises migrator with an sqlx client.
func NewMigratorWithSqlxClient(sx *sqlx.DB, sf string) *Migrator {
	if !checkFolders(sf) {
		fmt.Println("Folders and files are not organised acording to the documentation")
		return nil
	}

	schema := goyesql.MustParseFile(sf + schemaFileName)

	return &Migrator{
		SQLFolder: sf,
		DBC:       sx,
		sql:       schema,
	}
}

// CountMigrations - counts migrations in the migrations folder.
// Panics on error.
func (m *Migrator) CountMigrations() int {
	files, err := listFilesFilter(m.SQLFolder+migrationsFolderName, "*.sql")
	if err != nil {
		fmt.Printf("Error opening migrations directory: %s\n", err.Error())
		panic(err)
	}

	return len(files)
}

// ApplySchemaUp - uses schema_up sql and adds a migrations table.
// The migrations table is initialised with the current amount of migrations in the migrations folder and assumes the schema is up to date.
// Panics on error.
func (m *Migrator) ApplySchemaUp() {
	sq, ok := m.sql["schema_up"]
	if !ok {
		panic(fmt.Errorf("schemanot not found"))
	}

	tx := m.DBC.MustBegin()

	tx.MustExec(sq.Query)
	tx.MustExec(`
		CREATE TABLE IF NOT EXISTS migrations (
			id      SERIAL PRIMARY KEY,
			version INTEGER NOT NULL
		);`)
	tx.MustExec(`
		INSERT INTO migrations (id, version)
		VALUES (1, $1)
		ON CONFLICT (id)
		DO UPDATE SET version = EXCLUDED.version;
	`, m.CountMigrations())

	err := tx.Commit()
	if err != nil {
		if err := tx.Rollback(); err != nil {
			fmt.Printf("Rollback error: %s\n", err.Error())
			panic(fmt.Errorf("rollback error: %w", err))
		}
		fmt.Printf("Commit error: %s\n", err.Error())
		panic(fmt.Errorf("commit error: %w", err))
	}
}

// ApplySchemaDown - uses the schema_down SQL and removes migrations table
// Panics on error.
func (m *Migrator) ApplySchemaDown() {
	sq, ok := m.sql["schema_down"]
	if !ok {
		panic(fmt.Errorf("schemanot not found"))
	}

	tx := m.DBC.MustBegin()

	tx.MustExec(sq.Query)
	tx.MustExec(`DROP TABLE IF EXISTS migrations;`)

	err := tx.Commit()
	if err != nil {
		if err := tx.Rollback(); err != nil {
			fmt.Printf("Rollback error: %s\n", err.Error())
			panic(fmt.Errorf("rollback error: %w", err))
		}
		fmt.Printf("Commit error: %s\n", err.Error())
		panic(err)
	}
}

// ApplyMigration - applys any new migrations present in the migrations folders.
// Panics on error.
func (m *Migrator) ApplyMigration() {
	type row struct {
		ID      string `db:"id"`
		Version int    `db:"version"`
	}
	var r row
	err := m.DBC.QueryRowx("SELECT * FROM migrations WHERE id = 1").StructScan(&r)
	if err != nil {
		fmt.Printf("Error quering migrations table: %s\n", err.Error())
		panic(err)
	}

	fmt.Printf("Curent migration level: %d\n", r.Version)

	files, err := os.ReadDir(m.SQLFolder + "/migrations")
	if err != nil {
		fmt.Printf("Error opening migrations directory: %s\n", err.Error())
		panic(err)
	}

	sqlFiles := []fs.DirEntry{}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		split := strings.Split(file.Name(), ".")
		if len(split) <= 2 {
			continue
		}

		if split[1] != "sql" {
			continue
		}
		sqlFiles = append(sqlFiles, file)
	}

	if len(sqlFiles) == 0 {
		fmt.Println("No migrations")
		return
	}

	var toApply []int

	for _, sqlFile := range sqlFiles {
		split := strings.Split(sqlFile.Name(), ".")
		if len(split) == 2 {
			continue
		}

		level, err := strconv.Atoi(split[0])
		if err != nil {
			fmt.Printf("Could not parse migrations: %s\n", err.Error())
			panic(err)
		}

		if level > r.Version {
			toApply = append(toApply, level)
		}
	}

	if len(toApply) == 0 {
		fmt.Println("No new migrations")
		return
	}

	if len(toApply) > 1 {
		toApply = sortArray(toApply)
	}

	for _, l := range toApply {
		migration, err := os.ReadFile(fmt.Sprintf("%s%s%d.sql", m.SQLFolder, migrationsFolderName, l))
		if err != nil {
			fmt.Printf("Could not read migration file %d: %s\n", l, err.Error())
			panic(err)
		}

		tx, err := m.DBC.DB.Begin()
		if err != nil {
			fmt.Printf("Error begining transaction: %s\n", err.Error())
			panic(err)
		}

		_, err = tx.Exec(string(migration))
		if err != nil {
			fmt.Printf("Error executing migration itself: %s\n", err.Error())
			panic(err)
		}

		_, err = tx.Exec(fmt.Sprintf(`
			INSERT INTO migrations (id, version)
			VALUES (1, %d)
			ON CONFLICT (id)
			DO UPDATE SET version = EXCLUDED.version;
		`, l))
		if err != nil {
			fmt.Printf("Error executing version upgrate in db transaction: %s\n", err.Error())
			panic(err)
		}

		err = tx.Commit()
		if err != nil {
			fmt.Println("failed to commit transaction")
			panic(err)
		}

		fmt.Printf("Applied migration: %d\n", l)
		fmt.Printf("Upgraded migration version number: %d\n", l)
	}
}

func (m *Migrator) RunSQLScript(name string, args any) {
	sq, ok := m.sql[name]
	if !ok {
		fmt.Printf("SQL script '%s' not found", name)
		panic(fmt.Errorf("schemanot not found"))
	}

	_, err := m.DBC.Exec(sq.Query, args)
	if err != nil {
		fmt.Printf("exec failed on script %s with error: %s\n", name, err.Error())
		panic(fmt.Errorf("exec failed on script %s with error: %w", name, err))
	}
}
