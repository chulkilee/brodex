defmodule Brodex.MessageSet do
  @moduledoc """
  Represents a Kafka message set.

  Wrapper of `t::brod.message_set/0`.
  """

  @type t :: %__MODULE__{
          topic: Brodex.topic(),
          partition: Brodex.partition(),
          high_wm_offset: integer,
          messages: [Brodex.Message.t()] | {:incomplete_batch, Brodex.int32()}
        }

  @typedoc "`t::brod.message_set/0`"
  @type record ::
          {:kafka_message_set, topic :: Brodex.topic(), partition :: Brodex.partition(),
           high_wm_offset :: integer,
           messages :: [Brodex.Message.record()] | {:incomplete_batch, Brodex.int32()}}

  defstruct [:topic, :partition, :high_wm_offset, :messages]

  @doc """
  Converts a `t:record/0` into a `Brodex.MessageSet`.

  ## Examples

      iex>  Brodex.MessageSet.from_record(
      ...>    {:kafka_message_set, "my_topic", 0, 33,
      ...>    [
      ...>      {:kafka_message, 31, "", "a", :create, 1_564_023_091_657, []},
      ...>      {:kafka_message, 32, "", "b", :create, 1_564_023_091_894, []}
      ...>    ]}
      ...>  )
      %Brodex.MessageSet{
        high_wm_offset: 33,
        messages: [
          %Brodex.Message{
            headers: [],
            key: "",
            offset: 31,
            ts: 1_564_023_091_657,
            ts_type: :create,
            value: "a"
          },
          %Brodex.Message{
            headers: [],
            key: "",
            offset: 32,
            ts: 1_564_023_091_894,
            ts_type: :create,
            value: "b"
          }
        ],
        partition: 0,
        topic: "my_topic"
      }

  """
  @spec from_record(record) :: t
  def from_record(kafka_message_set_record)

  def from_record({:kafka_message_set, t, p, o, messages}),
    do: %__MODULE__{
      topic: t,
      partition: p,
      high_wm_offset: o,
      messages: Enum.map(messages, &cast_message/1)
    }

  defp cast_message(message) when is_tuple(message), do: Brodex.Message.from_record(message)

  defp cast_message(message), do: message

  @doc """
  Converts a `Brodex.MessageSet` into `t:record/0`.

  ### Examples

      iex> Brodex.MessageSet.to_record(%Brodex.MessageSet{
      ...>   topic: "hello",
      ...>   partition: 0,
      ...>   high_wm_offset: 1,
      ...>   messages: [
      ...>     %Brodex.Message{
      ...>       offset: 1,
      ...>       key: "",
      ...>       value: "world",
      ...>       ts_type: :undefined,
      ...>       ts: :undefined,
      ...>       headers: []
      ...>     }
      ...>   ]
      ...> })
      {:kafka_message_set, "hello", 0, 1,
       [{:kafka_message, 1, "", "world", :undefined, :undefined, []}]}

  """
  @spec to_record(t) :: record
  def to_record(%__MODULE__{} = struct) do
    {:kafka_message_set, struct.topic, struct.partition, struct.high_wm_offset,
     Enum.map(struct.messages, &Brodex.Message.to_record/1)}
  end
end
