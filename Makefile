LIBRDKAFKA_VER=1.4.4

.PHONY: test build
test:
	CGO_ENABLED=1 \
    CGO_LDFLAGS="-L/usr/local" \
	go test -tags=dynamic -count=1 -v ./...

install-librdkafka:
	# install librdkafka
	echo "Building librdkafka from source"
	cd /tmp
	git clone https://github.com/edenhill/librdkafka.git -b v${LIBRDKAFKA_VER}
	cd librdkafka
	./configure --install-deps
	make
	sudo make install
	cd ..
	rm -rf librdkafka

build:
	mkdir -p build 
	CGO_ENABLED=1 \
    CGO_LDFLAGS="-L/usr/local" \
	go build -o build/webhookd -tags=dynamic *.go 