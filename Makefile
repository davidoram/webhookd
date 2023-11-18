.PHONY: test build
test:
	CGO_ENABLED=1 \
    CGO_LDFLAGS="-L/usr/local" \
	go test -tags=dynamic -count=1 -v ./...

build:
	mkdir -p build 
	CGO_ENABLED=1 \
    CGO_LDFLAGS="-L/usr/local" \
	go build -o build/webhookd -tags=dynamic *.go 