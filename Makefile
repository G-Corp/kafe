include bu.mk

.PHONY: doc docker-compose.yml

compile-erl:
	$(verbose) $(REBAR) compile

compile-ex: elixir
	$(verbose) $(MIX) deps.get
	$(verbose) $(MIX) compile

elixir:
	$(verbose) $(REBAR) elixir generate_mix
	$(verbose) $(REBAR) elixir generate_lib

tests:
	$(verbose) $(REBAR) eunit

doc:
	$(verbose) $(REBAR) as doc edoc

lint:
	$(verbose) $(REBAR) lint

dist: dist-erl dist-ex doc lint

release: dist-ex dist-erl
	$(verbose) $(REBAR) hex publish

dist-erl: compile-erl tests

distclean-erl:
	$(verbose) rm -f rebar.lock

dist-ex: compile-ex

distclean-ex:
	$(verbose) rm -f mix.lock

distclean: distclean-ex distclean-erl
	$(verbose) rm -rf _build test/eunit deps ebin

dev: compile
	$(verbose) erl -pa _build/default/lib/*/ebin _build/default/lib/*/include -config config/kafe.config

KAFKA_ADVERTISED_HOST_NAME = $(shell ip addr list docker0 |grep "inet " |cut -d' ' -f6|cut -d/ -f1)

define docker_compose_yml_v1
version: "2"

services:
  zookeeper:
    image: dockerkafka/zookeeper
    ports:
      - "2181:2181"

  kafka1:
    image: wurstmeister/kafka
    ports:
      - "9092:9092"
    links:
      - zookeeper:zk
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zk
      KAFKA_BROKER_ID: 1
      KAFKA_ADVERTISED_HOST_NAME: $(KAFKA_ADVERTISED_HOST_NAME)
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock

  kafka2:
    image: wurstmeister/kafka
    ports:
      - "9093:9092"
    links:
      - zookeeper:zk
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zk
      KAFKA_BROKER_ID: 2
      KAFKA_ADVERTISED_HOST_NAME: $(KAFKA_ADVERTISED_HOST_NAME)
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock

  kafka3:
    image: wurstmeister/kafka
    ports:
      - "9094:9092"
    links:
      - zookeeper:zk
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zk
      KAFKA_BROKER_ID: 3
      KAFKA_ADVERTISED_HOST_NAME: $(KAFKA_ADVERTISED_HOST_NAME)
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock

  tools:
    image: confluent/tools
    depends_on:
      - zookeeper
      - kafka1
      - kafka2
      - kafka3
endef

docker-compose.yml:
	$(call render_template,docker_compose_yml_v1,docker-compose.yml)

docker-start: docker-stop
	$(verbose) docker-compose up -d
	$(verbose) sleep 1
	$(verbose) docker-compose run --rm tools kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 3 --partitions 3 --topic testone
	$(verbose) docker-compose run --rm tools kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 3 --partitions 3 --topic testtwo
	$(verbose) docker-compose run --rm tools kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 3 --partitions 3 --topic testthree

docker-stop: docker-compose.yml
	$(verbose) docker-compose kill
	$(verbose) docker-compose rm --all -vf

