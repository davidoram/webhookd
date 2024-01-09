FROM golang:1.21.3-alpine AS builder

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . .

# Build the Go application
RUN cd cmd/csv-generate && \
    CGO_ENABLED=1 go build \
    -tags musl \
    -v \
    -ldflags="-extldflags=-static" \
    -o /app/csv-generate *.go

# Use a minimal Alpine image as the base image
FROM alpine:latest

# Install SQLite
RUN apk --no-cache add sqlite

# Set the working directory in the container
WORKDIR /app

# Copy the compiled Go application from the builder image
COPY --from=builder /app/csv-generate /app/csv-generate

# Specify the entry point and default command to run the application
ENTRYPOINT ["/app/csv-generate"]

# Allow specifying CSV file on the /data external volume
CMD ["-csv", "/data/input.csv"]
