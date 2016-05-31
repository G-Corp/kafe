defmodule Kafe.Mixfile do
  use Mix.Project

  def project do
    [app: :kafe,
     version: "1.3.1",
     elixir: "~> 1.2",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps]
  end

  def application do
    [applications: [:syntax_tools,:compiler,:poolgirl,:goldrush,:lager], mod: {:kafe_app, []}]
  end

  defp deps do
    [ 
      {:lager, ~r/.*/, git: "https://github.com/basho/lager.git", tag: "3.2.0"},  
      {:bucs, ~r/.*/, git: "https://github.com/botsunit/bucs.git", tag: "0.0.1"},  
      {:doteki, ~r/.*/, git: "https://github.com/botsunit/doteki.git", tag: "0.0.1"},  
      {:poolgirl, ~r/.*/, git: "https://github.com/botsunit/poolgirl.git", tag: "0.0.1"},  
      {:bristow, ~r/.*/, git: "https://github.com/botsunit/bristow.git", tag: "0.0.1"},
    ]
  end
end
