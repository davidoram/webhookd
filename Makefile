LIBRDKAFKA_VER=1.4.4

.PHONY: test unit-test-run build
test: unit-test-setup unit-test-run unit-test-teardown

unit-test-run:
	CGO_ENABLED=1 \
    CGO_LDFLAGS="-L/usr/local" \
	go test -tags=dynamic -count=1 -v ./...

load-test: load-test-setup load-test-run load-test-teardown

load-test-run:
	# TODO Build a docker image and run it inside the docker-compose file 
	KAFKA_SERVERS=localhost:9092 \
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

clean:
	rm -rf build load-test/data
	mkdir -p load-test/data

build: 
	mkdir -p build 
	CGO_ENABLED=1 CGO_LDFLAGS="-L/usr/local" go build -o build/webhookd -tags=dynamic cmd/webhookd/*.go
	go build -o build/csv-generate -tags=dynamic cmd/csv-generate/*.go
	CGO_ENABLED=1 CGO_LDFLAGS="-L/usr/local" go build -o build/csv-publish -tags=dynamic cmd/csv-publish/*.go
	CGO_ENABLED=1 CGO_LDFLAGS="-L/usr/local" go build -o build/test-endpoint -tags=dynamic cmd/test-endpoint/*.go

dockerbuild-webhookd:
	docker build --progress=plain -t davidoram/webhookd -f cmd/webhookd/Dockerfile .
dockerbuild-csv-generate:
	docker build --progress=plain -t davidoram/csv-generate -f cmd/csv-generate/Dockerfile .
dockerbuild-csv-publish:
	docker build --progress=plain -t davidoram/csv-publish -f cmd/csv-publish/Dockerfile .
dockerbuild-test-endpoint:
	docker build --progress=plain -t davidoram/test-endpoint -f cmd/test-endpoint/Dockerfile .
dockerbuild: dockerbuild-webhookd dockerbuild-csv-generate dockerbuild-csv-publish dockerbuild-test-endpoint
	
load-test-build: build
	docker build -t davidoram/csv-publish -f cmd/csv-publish/Dockerfile .


coverage:
	mkdir -p build 
	go test -tags=dynamic -cover -coverprofile=build/c.out
	go tool cover -html="build/c.out"

unit-test-setup:
	docker-compose -f docker-compose.yml up --detach --build

unit-test-teardown:
	docker-compose -f docker-compose.yml down 	

load-test-setup:
	docker compose --file load-test/docker-compose.yml up --detach --force-recreate --remove-orphans

load-test-teardown:
	docker compose --file load-test/docker-compose.yml down 
	
curl-create-subscription:
	curl -X POST -H "Content-Type: application/json" -d @examples/new-subscription.json http://localhost:8080/1/subscriptions

curl-list-subscriptions:
	curl -X GET -H "Content-Type: application/json" http://localhost:8080/1/subscriptions
