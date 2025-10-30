defmodule Brodex.MixProject do
  use Mix.Project

  def project do
    [
      app: :brodex,
      version: "0.0.2",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      # hex
      description: "A thin wrapper of brod.",
      package: [
        licenses: ["Apache-2.0"],
        links: %{
          "GitHub" => "https://github.com/chulkilee/brodex",
          "Changelog" => "https://github.com/chulkilee/brodex/blob/master/CHANGELOG.md"
        }
      ],
      # docs
      source_url: "https://github.com/chulkilee/brodex"
    ]
  end

  def application do
    []
  end

  defp deps do
    [
      {:brod, "~> 4.0"},
      {:ex_doc, "~> 0.39.1", only: :dev, runtime: false}
    ]
  end
end
