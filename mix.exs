defmodule Kafe.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kafe,
      version: "1.5.2",
      elixir: "~> 1.2",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      deps: deps
    ]
  end

  def application do
    [
       applications: [:syntax_tools, :compiler, :poolgirl, :goldrush, :lager],
       env: [],
       mod: {:kafe_app, []}
    ]
  end

  defp deps do
    [
      {:lager, "~> 3.2"},
      {:bucs, "~> 0.1"},
      {:doteki, "~> 0.1"},
      {:poolgirl, "~> 0.1"},
      {:bristow, "~> 0.1"}    
    ]
  end
end