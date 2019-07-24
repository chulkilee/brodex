defmodule Brodex.MixProject do
  use Mix.Project

  def project do
    [
      app: :brodex,
      version: "0.1.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      # docs
      source_url: "https://github.com/chulkilee/brodex"
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.21.0", only: :dev, runtime: false}
    ]
  end
end
