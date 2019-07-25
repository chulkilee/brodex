defmodule Brodex.Message do
  @moduledoc """
  Represents a Kafka message.

  Wrapper of [`:brod.message`](https://hexdocs.pm/brod/brod.html#type-message).
  """

  require Record

  @type t :: %__MODULE__{
          offset: Brodex.offest(),
          key: Brodex.key(),
          value: Brodex.value(),
          ts_type: timestamp_type,
          ts: :undefined | Brodex.int64(),
          headers: Brodex.headers()
        }

  @typedoc "[`:brod.message`](https://hexdocs.pm/brod/brod.html#type-message)"
  @type record ::
          {:kafka_message, offset :: Brodex.offest(), key :: Brodex.key(),
           value :: Brodex.value(), ts_type :: timestamp_type, ts :: :undefined | Brodex.int64(),
           headers :: Brodex.headers()}

  @typedoc "[`:kpro.timestamp_type`](https://hexdocs.pm/kafka_protocol/kpro.html#type-timestamp_type)"
  @type timestamp_type :: :undefined | :create | :append

  defstruct [:offset, :key, :value, :ts_type, :ts, :headers]

  @doc """
  Converts a `t:record/0` into a `Brodex.Message`.

  ## Examples

      iex> Brodex.Message.from_record({:kafka_message, 164, "", "hello", :create, 1_563_946_803_056, []})
      %Brodex.Message{
        headers: [],
        key: "",
        offset: 164,
        ts: 1_563_946_803_056,
        ts_type: :create,
        value: "hello"
      }

      iex> Brodex.Message.from_record({:kafka_message, 164, "", "hello", :undefined, :undefined, []})
      %Brodex.Message{
        headers: [],
        key: "",
        offset: 164,
        ts: :undefined,
        ts_type: :undefined,
        value: "hello"
      }

  """
  @spec from_record(record) :: t
  def from_record(kafka_message_record)

  def from_record({:kafka_message, offset, k, v, ts_type, ts, headers}),
    do: %__MODULE__{offset: offset, key: k, value: v, ts_type: ts_type, ts: ts, headers: headers}
end
