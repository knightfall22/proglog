# START: begin
CONFIG_PATH=${HOME}/.proglog/

.PHONY: init
init:
	mkdir -p "${CONFIG_PATH}"

.PHONY: gencert
gencert:
	cfssl gencert \
		-initca certs/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=certs/ca-config.json \
		-profile=server \
		certs/server-csr.json | cfssljson -bare server

# START: client
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=certs/ca-config.json \
		-profile=client \
		certs/client-csr.json | cfssljson -bare client
# END: client

# START: multi_client
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=certs/ca-config.json \
		-profile=client \
		-cn="root" \
		certs/client-csr.json | cfssljson -bare root-client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=certs/ca-config.json \
		-profile=client \
		-cn="nobody" \
		certs/client-csr.json | cfssljson -bare nobody-client
# END: multi_client

	mv *.pem *.csr "${CONFIG_PATH}"

# END: begin
# START: auth
$(CONFIG_PATH)/model.conf:
	cp certs/model.conf "$(CONFIG_PATH)/model.conf"

$(CONFIG_PATH)/policy.csv:
	cp certs/policy.csv "$(CONFIG_PATH)/policy.csv"

# START: begin
.PHONY: test
# END: auth
test:
# END: begin
# START: auth
test: $(CONFIG_PATH)/policy.csv $(CONFIG_PATH)/model.conf
#: START: begin
	go test -race ./... -debug=true
# END: auth

.PHONY: compile
compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

# END: begin`


TAG ?= 0.0.1
build-docker:
	docker build -t github.com/knightfall22/proglog:$(TAG) .