build:
	go build -o migrator cmd/migrator/main.go

install: 
	go install -ldflags='-s -w -X "github.com/JamesTiberiusKirk/migrator.Version=development"' ./cmd/migrator

ver:
	go generate
