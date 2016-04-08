defmodule Kafe.Mixfile do
	use Mix.Project

	def project do
		[app: :kafe,
		 version: "1.2.0",
		 elixir: "~> 1.2",
		 build_embedded: Mix.env == :prod,
		 start_permanent: Mix.env == :prod,
		 deps: deps]
	end

	def application do
		[applications: [:syntax_tools,:compiler,:goldrush,:lager], mod: {:kafe_app, []}]
	end

	defp deps do
		[ 
			{:lager, ~r/.*/, git: "https://github.com/basho/lager.git", branch: "master"},  
			{:bucs, ~r/.*/, git: "https://github.com/botsunit/bucs.git", branch: "master"},  
			{:doteki, ~r/.*/, git: "https://github.com/botsunit/doteki.git", branch: "master"},
		]
	end
end
