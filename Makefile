PROJECT = kafe

DEPS = lager bucs doteki
dep_lager = git https://github.com/basho/lager.git master
dep_bucs = git https://github.com/botsunit/bucs.git master
dep_doteki = git https://github.com/botsunit/doteki.git master

DOC_DEPS = edown
dep_edown = git https://github.com/botsunit/edown.git master

include erlang.mk

ERLC_OPTS = +debug_info +'{parse_transform, lager_transform}'

EDOC_OPTS = {doclet, edown_doclet} \
						, {app_default, "http://www.erlang.org/doc/man"} \
						, {source_path, ["src"]} \
						, {overview, "overview.edoc"} \
						, {stylesheet, ""} \
						, {image, ""} \
						, {top_level_readme, {"./README.md", "https://github.com/botsunit/kafe"}}

dev: deps app
	@erl -pa ebin include deps/*/ebin deps/*/include -config config/kafe.config

