package main

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"

	m "github.com/JamesTiberiusKirk/migrator"
	"github.com/JamesTiberiusKirk/migrator/migrator"
	"github.com/joho/godotenv"
)

func obfuscatePassword(connURL string) string {
	parsedURL, err := url.Parse(connURL)
	if err != nil {
		panic("failed to obfuscate password")
	}
	username := parsedURL.User.Username()
	parsedURL.User = url.UserPassword(username, "xxxxxxx")
	return parsedURL.String()
}

func getPGUrl() string {
	url := os.Getenv("DB_URL")
	if url == "" {
		dbName := os.Getenv("DB_NAME")
		dbUser := os.Getenv("DB_USER")
		dbPass := os.Getenv("DB_PASS")
		dbHost := os.Getenv("DB_HOST")
		disableSSL := os.Getenv("DB_DISABLE_SSL") == "true"

		if dbName == "" || dbUser == "" || dbPass == "" || dbHost == "" {
			panic("Need either DB_URL or DB_USER + DB_PASS + DB_HOST + DB_NAME")
		}

		url = fmt.Sprintf("postgres://%s:%s@%s/%s", dbUser, dbPass, dbHost, dbName)
		if disableSSL {
			url += "?sslmode=disable"
		}
	}
	return url
}

func main() {
	fmt.Println("------------------------------------------------------------")
	fmt.Println("MIGRATOR")

	osArgs := os.Args[1:]
	if len(osArgs) <= 0 {
		fmt.Println("Usage:")
		fmt.Println("[version|check|count-migrations|schema-up|schema-down|schema-reload|migrate|run <script_name>]")
		return
	}

	command := osArgs[0]

	switch command {
	case "version":
		fmt.Printf("Version: %s\n", m.Version)
		return
	}

	if err := godotenv.Load(); err != nil {
		fmt.Println("No .env file found, using actual env")
	}

	sqlFolderPath := os.Getenv("MIGRATOR_SQL_FOLDER_PATH")
	if sqlFolderPath == "" {
		sqlFolderPath = "./sql/"
	}

	dbURL := getPGUrl()

	switch command {
	case "check":
		db, err := sql.Open("postgres", dbURL)
		if err != nil {
			fmt.Printf("Failed to connect to the database: %s\n", err)
			return
		}
		if err := db.Ping(); err != nil {
			fmt.Printf("Failed to ping database: %s\n", err)
			return
		}
		fmt.Println("Database connection successful")

		m, err := migrator.NewMigratorWithSqlClient(db, sqlFolderPath)
		if err != nil {
			fmt.Printf("Migrator error: %s\n", err)
			return
		}
		init, err := m.IsInitialised()
		if err != nil {
			fmt.Printf("Init check failed: %s\n", err)
			return
		}
		fmt.Printf("Migrator initialised: %t\n", init)

	case "count-migrations":
		m, err := migrator.NewMigratorWithPostgresURL(dbURL, sqlFolderPath)
		if err != nil {
			fmt.Printf("Failed to init migrator: %s\n", err)
			return
		}
		count, err := m.CountMigrations()
		if err != nil {
			fmt.Printf("Failed to count migrations: %s\n", err)
			return
		}
		fmt.Printf("Available migrations: %d\n", count)

	case "schema-up":
		fmt.Printf("Applying schema up to db: %s\n", obfuscatePassword(dbURL))
		m, err := migrator.NewMigratorWithPostgresURL(dbURL, sqlFolderPath)
		if err != nil {
			fmt.Printf("Failed to init migrator: %s\n", err)
			return
		}
		if err := m.ApplySchemaUp(); err != nil {
			fmt.Printf("Schema up failed: %s\n", err)
		}

	case "schema-down":
		fmt.Printf("Applying schema down to db: %s\n", obfuscatePassword(dbURL))
		m, err := migrator.NewMigratorWithPostgresURL(dbURL, sqlFolderPath)
		if err != nil {
			fmt.Printf("Failed to init migrator: %s\n", err)
			return
		}
		if err := m.ApplySchemaDown(); err != nil {
			fmt.Printf("Schema down failed: %s\n", err)
		}

	case "schema-reload":
		fmt.Printf("Reloading schema on db: %s\n", obfuscatePassword(dbURL))
		m, err := migrator.NewMigratorWithPostgresURL(dbURL, sqlFolderPath)
		if err != nil {
			fmt.Printf("Failed to init migrator: %s\n", err)
			return
		}
		if err := m.ApplySchemaDown(); err != nil {
			fmt.Printf("Schema down failed: %s\n", err)
			return
		}
		if err := m.ApplySchemaUp(); err != nil {
			fmt.Printf("Schema up failed: %s\n", err)
		}

	case "migrate":
		fmt.Printf("Applying migration to db: %s\n", obfuscatePassword(dbURL))
		m, err := migrator.NewMigratorWithPostgresURL(dbURL, sqlFolderPath)
		if err != nil {
			fmt.Printf("Failed to init migrator: %s\n", err)
			return
		}
		if err := m.ApplyMigration(); err != nil {
			fmt.Printf("Migration failed: %s\n", err)
		}

	case "run":
		if len(osArgs) < 2 {
			fmt.Println("Need to provide the name of a script to run")
			return
		}
		script := osArgs[1]
		m, err := migrator.NewMigratorWithPostgresURL(dbURL, sqlFolderPath)
		if err != nil {
			fmt.Printf("Failed to init migrator: %s\n", err)
			return
		}
		if err := m.RunSQLScript(script); err != nil {
			fmt.Printf("Failed to run script: %s\n", err)
		}

	default:
		fmt.Println("Unknown command. Valid commands are:")
		fmt.Println("[version|check|count-migrations|schema-up|schema-down|schema-reload|migrate|run <script_name>]")
	}

	fmt.Println("------------------------------------------------------------")
}
