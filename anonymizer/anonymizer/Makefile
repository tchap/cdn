.PHONY: build gen

build:
	go build -o build/anonymizer

gen:
	capnp compile -I ./go-capnp/std -ogo internal/messages/http_log.capnp
