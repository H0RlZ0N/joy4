
all:
	gofmt -d -l -e -w .
	go mod tidy
	go build

run:
	rtmp_publish.exe
	