REBAR = ./rebar3

compile:
	@$(REBAR) compile

tests:
	@$(REBAR) eunit

doc:
	@$(REBAR) as doc edoc

release: compile tests
	@$(REBAR) elixir generate_mix
	@$(REBAR) elixir generate_lib

distclean:
	@rm -rf _build rebar.lock mix.lock

