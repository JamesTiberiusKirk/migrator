package main

import (
	"fmt"
	"net/url"
	"os"

	"github.com/JamesTiberiusKirk/migrator"
	"github.com/joho/godotenv"
)

func obfuscatePassword(connURL string) (string, error) {
	parsedURL, err := url.Parse(connURL)
	if err != nil {
		return "", err
	}

	username := parsedURL.User.Username()
	parsedURL.User = url.UserPassword(username, "xxxxxxx")

	return parsedURL.String(), nil
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
		fmt.Println("[count-migrations|schema-up|schema-down|migrate|run <script_name>]")
		return
	}

	url := os.Getenv("DB_URL")

	m := migrator.NewMigratorWithPostgresURL(url, "./sql/")

	switch osArgs[0] {
	case "count-migrations":
		fmt.Println("Available migrations")
		fmt.Println(m.CountMigrations())
	case "schema-up":
		fmt.Printf("Applying schema up to db: %s\n", url)
		m.ApplySchemaUp()
	case "schema-down":
		fmt.Printf("Applying schema down to db: %s\n", url)
		m.ApplySchemaDown()
	case "schema-reload":
		fmt.Printf("Applying schema down to db: %s\n", url)
		m.ApplySchemaDown()
		fmt.Printf("Applying schema up to db: %s\n", url)
		m.ApplySchemaUp()
	case "migrate":
		fmt.Printf("Applying migration to db: %s\n", url)
		m.ApplyMigration()
	case "run":
		if len(osArgs) <= 1 {
			fmt.Println("Need to privide the name of a script to run")
			return
		}

		fmt.Printf("Running SQL script on db: %s\n", url)
		m.RunSQLScript(osArgs[1], nil)
	default:
		fmt.Println("Please provide one of the following")
		fmt.Println("[count-migrations|schema-up|schema-down|migrate|run <script_name>]")
	}
	fmt.Print("------------------------------------------------------------")
}
