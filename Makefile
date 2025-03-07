build:
	go build -o migrator cmd/migrator/main.go

install: 
	go install ./cmd/migrator

ver:
	./get_version.sh
