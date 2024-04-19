.PHONY: build gen

build:
	go build -o build/anonymizer

# For gen to work, you need to temporarily clone https://github.com/capnproto/go-capnp
# into this directory. Follow the instructions there to install all dependencies.
# To learn more: https://github.com/capnproto/go-capnp/blob/main/docs/Writing-Schemas-and-Generating-Code.md
gen:
	capnp compile -I ./go-capnp/std -ogo internal/messages/http_log.capnp
