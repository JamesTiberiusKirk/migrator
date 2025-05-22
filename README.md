# MIGRATOR Database Migration Utility  
Works both as a package to be used at the start of your application or as a cli tool to be manually ran.
For the moment this is made only for Postgres

sql path needs to look like the following
```
sql/
|---migrations/
|   |---1.sql
|   |---2.sql
|   |---[.].sql
|---schema.sql
```

- `schema.sql` is expected to be a [goyesql](github.com/knadh/goyesql) valid file with `schema_up`, `schema_down` and any other scripts that are wanted to be ran by the cli/lib.
- The sql files inside migrations folder are just plain sql files.
    - The file naming convention **MUST** be a sequantial valid number
    - Any non`*.sql`  files will be ignored

## CLI 
```sh
go install github.com/JamesTiberiusKirk/migrator/cmd/migrator@latest
migrator schema-up
migrator schema-down
migrator schema-reload
migrator migrate
migrator count-migrations 
migrator run "sql_script_name"
```

The cli needs some env variables also. Either the full db url string:
```
DB_URL
```

Or the following so it can build its own postgres url string

```
DB_NAME=databaseName
DB_USER=user
DB_PASS=password
DB_HOST=localhost:5432
DB_DISABLE_SSL=true
```

Also a var for the sql path if you want it to be different from the default of `./sql/`
```
MIGRATOR_SQL_FOLDER_PATH=./whatever/path/you/want
```

## PACKAGE
```sh
go get github.com/JamesTiberiusKirk/migrator@latest
```
View `cmd/migrator/main.go` for examples.


## Future works
- Support multiple databases/make it modular so database support can be injected
