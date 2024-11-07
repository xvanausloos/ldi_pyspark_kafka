ifndef STAGE
$(error STAGE is not set)
endif

install-requirements:
	pip install -r requirements.txt

deploy-kafka-cluster:
	kind create cluster --config infrastructure/kind-config.yml --name ldi-kafka-cluster
	kubectl cluster-info --context kind-ldi-kafka-cluster

