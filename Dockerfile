# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS builder
WORKDIR /src

RUN apk add --no-cache ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM alpine:3.21
WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata wget \
 && adduser -D -u 10001 app

COPY --from=builder /out/api /app/api
COPY --from=builder /out/worker /app/worker

USER app
EXPOSE 8080

ENTRYPOINT ["/app/api"]
