PROJECT = kafe
DEPS = lager edown eutils
dep_lager = git https://github.com/basho/lager.git master
dep_edown = git https://github.com/homeswap/edown.git master
dep_eutils = git https://github.com/emedia-project/eutils.git master
include erlang.mk

ERLC_OPTS = +debug_info +'{parse_transform, lager_transform}'
EDOC_OPTS = {doclet, edown_doclet} \
						, {app_default, "http://www.erlang.org/doc/man"} \
						, {new, true} \
						, {packages, false} \
						, {stylesheet, ""} \
						, {image, ""} \
						, {top_level_readme, {"./documentation.md", "https://github.com/nexkap/kafe"}} 

dev: deps app
	@erl -pa ebin include deps/*/ebin deps/*/include -config config/kafe.config

