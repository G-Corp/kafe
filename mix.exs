defmodule Kafe.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kafe,
      version: "1.3.1",
      elixir: "~> 1.2",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      deps: deps
    ]
  end

  def application do
    [
       applications: [:kernel, :stdlib, :syntax_tools, :compiler, :poolgirl, :goldrush, :lager],
       env: [],
       mod: {:kafe_app, []}
    ]
  end

  defp deps do
    [
      {:lager, "~> 3.2.0"},
      {:bucs, git: "https://github.com/botsunit/bucs.git", branch: "master"},
      {:doteki, git: "https://github.com/botsunit/doteki.git", branch: "master"},
      {:poolgirl, git: "https://github.com/botsunit/poolgirl.git", branch: "master"},
      {:bristow, git: "https://github.com/botsunit/bristow.git", branch: "master"}    
    ]
  end
end