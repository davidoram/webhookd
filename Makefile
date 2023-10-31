_LIGHT_TEAL:=1;36

test:
	docker run --rm \
	--env LD_LIBRARY_PATH=/usr/local/lib \
	--env CGO_ENABLED=1 \
	--env CGO_LDFLAGS=-L/usr/local \
	-v "$(PWD)":/usr/src/myapp -w /usr/src/myapp davidoram/go-kafka-client:1.4.4 \
	/bin/bash -c "go test -tags=dynamic -count=1 -v ./..."

shell:
	docker run --rm \
	--env LD_LIBRARY_PATH=/usr/local/lib \
	--env CGO_ENABLED=1 \
	--env CGO_LDFLAGS=-L/usr/local \
	--interactive \
	-v "$(PWD)":/usr/src/myapp -w /usr/src/myapp davidoram/go-kafka-client:1.4.4 \
	/bin/bash -c "echo 'In container:'; echo '  $$PWD'; /bin/bash"