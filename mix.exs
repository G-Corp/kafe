defmodule Kafe.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kafe,
      version: "1.4.1",
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
      {:lager, "~> 3.2.0"},
      {:bucs, git: "https://github.com/botsunit/bucs.git", tag: "0.0.2"},
      {:doteki, git: "https://github.com/botsunit/doteki.git", tag: "0.1.0"},
      {:poolgirl, git: "https://github.com/botsunit/poolgirl.git", tag: "0.0.2"},
      {:bristow, git: "https://github.com/botsunit/bristow.git", tag: "0.0.2"}    
    ]
  end
end