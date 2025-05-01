package migrator

import (
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/knadh/goyesql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validTestQslPath = "./test_dirs/sql_1/"

func setupMockMigrator(t *testing.T, schema goyesql.Queries, sqlPath string) (*Migrator, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	m := &Migrator{
		DBC:                  db,
		sql:                  schema,
		SQLFolder:            sqlPath,
		schemaFileName:       "schema.sql",
		migrationsFolderName: "migrations/",
	}

	return m, mock
}

func TestNewMigratorWithSqlClient(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		db, _, err := sqlmock.New()
		assert.NoError(t, err)
		defer db.Close()

		m, err := NewMigratorWithSqlClient(db, validTestQslPath)
		assert.NoError(t, err)
		assert.NotNil(t, m)
		assert.Equal(t, validTestQslPath, m.SQLFolder)
	})

	t.Run("Fail_InvalidPath", func(t *testing.T) {
		db, _, err := sqlmock.New()
		assert.NoError(t, err)
		defer db.Close()

		_, err = NewMigratorWithSqlClient(db, "./invalid_path/")
		assert.Error(t, err)
	})
}

func TestIsInitialised(t *testing.T) {
	tests := []struct {
		name        string
		rows        int
		expectInit  bool
		expectError error
	}{
		{
			name:        "initialised_migrator",
			rows:        1,
			expectInit:  true,
			expectError: nil,
		},
		{
			name:        "uninitialised_migrator",
			rows:        0,
			expectInit:  false,
			expectError: nil,
		},
		{
			name:        "uninitialised_migrator_on_error",
			rows:        0,
			expectInit:  false,
			expectError: fmt.Errorf("error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, mock := setupMockMigrator(t, goyesql.Queries{}, validTestQslPath)

			mock.ExpectQuery("SELECT COUNT\\(tablename\\) FROM pg_tables WHERE tablename = 'migrations';").
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(tt.rows)).WillReturnError(tt.expectError)

			init, err := m.IsInitialised()
			if err != nil {
				assert.ErrorContains(t, err, tt.expectError.Error())
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.expectInit, init)
		})
	}
}

func TestApplySchemaUp(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		schema := goyesql.Queries{
			"schema_up": {Query: "CREATE TABLE users (id SERIAL PRIMARY KEY);"},
		}
		m, mock := setupMockMigrator(t, schema, validTestQslPath)

		mock.ExpectQuery("SELECT COUNT\\(tablename\\) FROM pg_tables WHERE tablename = 'migrations';").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

		mock.ExpectBegin()
		mock.ExpectExec("CREATE TABLE users").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS migrations").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("INSERT INTO migrations").WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		err := m.ApplySchemaUp()
		assert.NoError(t, err)
	})

	t.Run("AlreadyInitialised", func(t *testing.T) {
		schema := goyesql.Queries{
			"schema_up": {Query: "CREATE TABLE users (id SERIAL PRIMARY KEY);"},
		}
		m, mock := setupMockMigrator(t, schema, validTestQslPath)

		mock.ExpectQuery("SELECT COUNT\\(tablename\\) FROM pg_tables WHERE tablename = 'migrations';").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		err := m.ApplySchemaUp()
		assert.ErrorContains(t, err, "schema is already initialised")
	})

	t.Run("MissingSchemaUp", func(t *testing.T) {
		m, mock := setupMockMigrator(t, goyesql.Queries{}, validTestQslPath)

		mock.ExpectQuery("SELECT COUNT\\(tablename\\) FROM pg_tables WHERE tablename = 'migrations';").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

		err := m.ApplySchemaUp()
		assert.ErrorContains(t, err, "schema_up not found")
	})
}

func TestApplySchemaDown(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		schema := goyesql.Queries{
			"schema_down": {Query: "DROP TABLE users;"},
		}
		m, mock := setupMockMigrator(t, schema, validTestQslPath)

		mock.ExpectBegin()
		mock.ExpectExec("DROP TABLE users").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("DROP TABLE IF EXISTS migrations").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectCommit()

		err := m.ApplySchemaDown()
		assert.NoError(t, err)
	})

	t.Run("MissingSchemaDown", func(t *testing.T) {
		m, _ := setupMockMigrator(t, goyesql.Queries{}, validTestQslPath)

		err := m.ApplySchemaDown()
		assert.ErrorContains(t, err, "schema_down not found")
	})
}

func TestApplyMigration(t *testing.T) {
	t.Run("ApplyFirstMigration", func(t *testing.T) {
		schema := goyesql.Queries{}
		m, mock := setupMockMigrator(t, schema, validTestQslPath)

		mock.ExpectQuery("SELECT version FROM migrations WHERE id = 1").
			WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(0))

		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO users \\(name\\) VALUES \\('test user'\\);").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO migrations").
			WithArgs(1).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		err := m.ApplyMigration()
		assert.NoError(t, err)
	})
}

func TestRunSQLScript(t *testing.T) {
	t.Run("Success_WithArgs", func(t *testing.T) {
		schema := goyesql.Queries{
			"test_script": {Query: "INSERT INTO users (name) VALUES ($1)"},
		}
		m, mock := setupMockMigrator(t, schema, validTestQslPath)

		mock.ExpectExec("INSERT INTO users").WithArgs("John").
			WillReturnResult(sqlmock.NewResult(1, 1))

		err := m.RunSQLScript("test_script", "John")
		assert.NoError(t, err)
	})

	t.Run("MissingScript", func(t *testing.T) {
		m, _ := setupMockMigrator(t, goyesql.Queries{}, validTestQslPath)

		err := m.RunSQLScript("not_found")
		assert.ErrorContains(t, err, "not found")
	})
}
