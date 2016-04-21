PROJECT = kafe

DEP_PLUGINS = mix.mk
BUILD_DEPS = mix.mk bristow
ELIXIR_VERSION = ~> 1.2
ELIXIR_BINDINGS = kafe

dep_mix.mk = git https://github.com/botsunit/mix.mk.git master
dep_bristow = git https://github.com/botsunit/bristow.git master

DEPS = lager bucs doteki poolgirl
dep_lager = git https://github.com/basho/lager.git master
dep_bucs = git https://github.com/botsunit/bucs.git master
dep_doteki = git https://github.com/botsunit/doteki.git master
dep_poolgirl = git https://github.com/botsunit/poolgirl.git master

DOC_DEPS = edown
dep_edown = git https://github.com/botsunit/edown.git master

app::

tests::
	@mkdir -p test/ct
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
CT_OPTS = -ct_hooks cth_surefire -logdir test/ct

dev: deps app
	@erl -pa ebin include deps/*/ebin deps/*/include -config config/kafe.config

release: app mix.all

distclean::
	@rm -rf log
	@rm -rf test/eunit
	@rm -rf test/ct
	@rm -f test/*.beam

