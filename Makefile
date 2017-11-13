HAS_ELIXIR=1
# NO_REGISTRY_UPDATE=1

include bu.mk

release: dist lint tag ## Tag and release to hex.pm
	$(verbose) $(REBAR) hex publish

integ: ## Run integration tests
	$(verbose) $(REBAR) ct

tests-all: tests integ

check-etchosts:
	@grep kafka1 /etc/hosts >/dev/null || \
	printf 'Please add the following line to your /etc/hosts:\n\
	  127.0.0.1 kafka1 kafka2 kafka3\n\
	'

define one_kafka
  kafka$(1):
    image: confluentinc/cp-kafka:3.3.0-1
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: $(1)
      KAFKA_ADVERTISED_LISTENERS: plaintext://kafka$(1):919$(1)
      KAFKA_ZOOKEEPER_CONNECT: zookeeper
      KAFKA_MESSAGE_MAX_BYTES: 1000000
      KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS: 6
    ports:
      - "919$(1):919$(1)"

endef

define docker_compose_yml_v1
version: "2"

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:3.3.0-1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

$(call one_kafka,1)
$(call one_kafka,2)
$(call one_kafka,3)
endef

docker-compose.yml: Makefile ## Create docker-compose.yml
	$(call render_template,docker_compose_yml_v1,docker-compose.yml)

# Commands to create the topics, and force auto-creation of the __consumer_offsets topic
define topic_commands
  kafka-topics --create --zookeeper zookeeper:2181 --replica-assignment 1 --topic testone
  kafka-topics --create --zookeeper zookeeper:2181 --replica-assignment 1:2,2:1 --topic testtwo
  kafka-topics --create --zookeeper zookeeper:2181 --replica-assignment 1:2:3,2:3:1,3:1:2 --topic testthree
  kafka-console-consumer --bootstrap-server localhost:9191 --new-consumer --timeout-ms 1 --topic testone 2>/dev/null
endef

docker-start: docker-compose.yml docker-stop check-etchosts ## (Re)Start docker
	$(verbose) docker-compose up -d
	$(verbose) sleep 5 # give cluster time to startup
	$(verbose) docker-compose exec kafka1 bash -ec '$(subst $(newline),&&,$(topic_commands))'

docker-stop: docker-compose.yml ## Stop docker
	$(verbose) docker-compose kill
	$(verbose) docker-compose rm -svf
