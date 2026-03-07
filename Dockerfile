FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o visualizer ./cmd/visualizer/

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/visualizer .
EXPOSE 8080
CMD ["./visualizer"]
