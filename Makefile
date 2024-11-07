ifndef STAGE
$(error STAGE is not set)
endif

install-requirements:
	pip install -r requirements.txt

create-kafka-cluster:
	kind create cluster --config infrastructure/kind-config.yml --name ldi-kafka-cluster
	kubectl cluster-info --context kind-ldi-kafka-cluster

install-kafka:
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm install kafka-local bitnami/kafka --set persistence.enabled=false,zookeeper.persistence.enabled=false

kafka-port-forward:
	`kubectl port-forward svc/kafka-local 9092:9092 -n default`

install-kafka-locally:
	`brew install kafka`




