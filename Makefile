linux:fmt
	rm -rf ./cmd/go_mysql2mysql
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./cmd/go_mysql2mysql ./cmd/main.go
mac:fmt
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o ./cmd/go_mysql2mysql_mac ./cmd/main.go
fmt:
	gofmt -l -w cmd
	gofmt -l -w config
	gofmt -l -w internal
