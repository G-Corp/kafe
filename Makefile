PROJECT = kafe

DEP_PLUGINS = mix.mk
BUILD_DEPS = mix.mk
ELIXIR_VERSION = ~> 1.2
ELIXIR_BINDINGS = kafe kafe_consumer
dep_mix.mk = git https://github.com/botsunit/mix.mk.git master

DEPS = lager bucs doteki poolgirl bristow
dep_lager = git https://github.com/basho/lager.git 3.2.0
dep_bucs = git https://github.com/botsunit/bucs.git 0.0.1
dep_doteki = git https://github.com/botsunit/doteki.git 0.0.1
dep_poolgirl = git https://github.com/botsunit/poolgirl.git 0.0.1
dep_bristow = git https://github.com/botsunit/bristow.git 0.0.1

DOC_DEPS = edown
dep_edown = git https://github.com/botsunit/edown.git master

TEST_DEPS = meck
dep_meck = git https://github.com/eproxus/meck.git master

app::

tests::
	@mkdir -p test/eunit

include erlang.mk

EDOC_OPTS = {doclet, edown_doclet} \
						, {app_default, "http://www.erlang.org/doc/man"} \
						, {source_path, ["src"]} \
						, {overview, "overview.edoc"} \
						, {stylesheet, ""} \
						, {image, ""} \
						, {top_level_readme, {"./README.md", "https://github.com/botsunit/kafe"}}

EUNIT_OPTS = verbose, {report, {eunit_surefire, [{dir, "test/eunit"}]}}

dev: deps app
	@erl -pa ebin include deps/*/ebin deps/*/include -config config/kafe.config

release: app mix.all

distclean::
	@rm -rf log
	@rm -rf test/eunit
	@rm -f test/*.beam

KAFKA_ADVERTISED_HOST_NAME = $(shell docker network inspect bridge | grep Gateway | awk -F: '{print $$2}' | sed -e 's/^\s*"//' | sed -e 's/".*$$//')

define docker_compose_yml_v2
version: "2"

services:
  zookeeper:
    image: confluent/zookeeper
    ports:
      - "2181:2181"

  kafka1:
    image: confluent/kafka
    ports:
      - "9092:9092"
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ADVERTISED_HOST_NAME: $(KAFKA_ADVERTISED_HOST_NAME)
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"

  kafka2:
    image: confluent/kafka
    ports:
      - "9093:9092"
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ADVERTISED_HOST_NAME: $(KAFKA_ADVERTISED_HOST_NAME)
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"

  kafka3:
    image: confluent/kafka
    ports:
      - "9094:9092"
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 3
      KAFKA_ADVERTISED_HOST_NAME: $(KAFKA_ADVERTISED_HOST_NAME)
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"

  tools:
    image: confluent/tools
    depends_on:
      - zookeeper
      - kafka1
      - kafka2
      - kafka3

endef

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
	@docker-compose up -d
	@sleep 1
	@docker-compose run --rm tools kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 3 --partitions 3 --topic testone
	@docker-compose run --rm tools kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 3 --partitions 3 --topic testtwo
	@docker-compose run --rm tools kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 3 --partitions 3 --topic testthree

docker-stop: docker-compose.yml
	@docker-compose kill
	@docker-compose rm --all -vf

