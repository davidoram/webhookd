# Use a Golang runtime that includes the Kafka client library to build the application
FROM davidoram/go-kafka-client:1.4.4 AS builder

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . .

# Build the Go application
RUN cd cmd/webhookd && \
    CGO_ENABLED=1 go build \
    -tags musl \
    -v \
    -ldflags="-extldflags=-static" \
    -o /app/webhookd *.go

# Use a minimal Alpine image as the base image
FROM alpine:latest

# Install SQLite
RUN apk --no-cache add sqlite

# Set the working directory in the container
WORKDIR /app

# Copy the compiled Go application from the builder image
COPY --from=builder /app/webhookd /app/webhookd

# Specify the entry point and default command to run the application
ENTRYPOINT ["/app/webhookd"]

# By default specify SQLite database files on external volume /data
CMD ["-db", "file:/data/webhookd.db?vacuum=1"]
