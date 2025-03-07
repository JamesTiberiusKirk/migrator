package main

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"

	"github.com/JamesTiberiusKirk/migrator"
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

	err := godotenv.Load()
	if err != nil {
		fmt.Println("No .env getting from actual env")
	}

	osArgs := os.Args[1:]

	if len(osArgs) <= 0 {
		fmt.Println("need to provide one of the following")
		fmt.Println("[version|count-migrations|schema-up|schema-down|migrate|run <script_name>]")
		return
	}

	sqlFolderPath := os.Getenv("MIGRATOR_SQL_FOLDER_PATH")
	if sqlFolderPath == "" {
		sqlFolderPath = "./sql/"
	}

	switch osArgs[0] {
	case "version":
		fmt.Printf("Version: %s\n", migrator.Version)

	case "check":
		url := getPGUrl()

		db, err := sql.Open("postgres", obfuscatePassword(url))
		if err != nil {
			fmt.Printf("Failed to connect to the database: %s", err.Error())
			return
		}

		if err := db.Ping(); err != nil {
			fmt.Printf("Failed to ping database")
			return
		}

		fmt.Println("Database connection successful")
		fmt.Printf("Version: %s\n", migrator.Version)
		fmt.Printf("SQL Path: %s\n", sqlFolderPath)
	case "count-migrations":
		fmt.Println("Available migrations")
		url := getPGUrl()
		m := migrator.NewMigratorWithPostgresURL(url, sqlFolderPath)
		fmt.Println(m.CountMigrations())
	case "schema-up":
		url := getPGUrl()
		fmt.Printf("Applying schema up to db: %s\n", obfuscatePassword(url))
		m := migrator.NewMigratorWithPostgresURL(url, sqlFolderPath)
		m.ApplySchemaUp()
	case "schema-down":
		url := getPGUrl()
		fmt.Printf("Applying schema down to db: %s\n", obfuscatePassword(url))
		m := migrator.NewMigratorWithPostgresURL(url, sqlFolderPath)
		m.ApplySchemaDown()
	case "schema-reload":
		url := getPGUrl()
		fmt.Printf("Applying schema down to db: %s\n", obfuscatePassword(url))
		m := migrator.NewMigratorWithPostgresURL(url, sqlFolderPath)
		m.ApplySchemaDown()
		fmt.Printf("Applying schema up to db: %s\n", obfuscatePassword(url))
		m.ApplySchemaUp()
	case "migrate":
		url := getPGUrl()
		fmt.Printf("Applying migration to db: %s\n", obfuscatePassword(url))
		m := migrator.NewMigratorWithPostgresURL(url, sqlFolderPath)
		m.ApplyMigration()
	case "run":
		if len(osArgs) <= 1 {
			fmt.Println("Need to privide the name of a script to run")
			return
		}

		url := getPGUrl()
		m := migrator.NewMigratorWithPostgresURL(url, sqlFolderPath)
		fmt.Printf("Running SQL script on db: %s\n", obfuscatePassword(url))
		m.RunSQLScript(osArgs[1], nil)
	default:
		fmt.Println("Please provide one of the following")
		fmt.Println("[count-migrations|schema-up|schema-down|migrate|run <script_name>]")
	}
	fmt.Println("------------------------------------------------------------")
}
