FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY internal internal
COPY main.go main.go

RUN go build -o anonymizer

FROM alpine:3.19 
RUN apk add ca-certificates

COPY --from=builder /app/anonymizer /app/anonymizer

CMD ["/app/anonymizer"]