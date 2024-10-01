defmodule Brodex.TopicSubscriber do
  @moduledoc """
  Wrapper of `m::brod_topic_subscriber`.
  """

  @callback init(topic :: Brodex.topic(), term) ::
              {:ok, committed_offsets, :brod_topic_subscriber.cb_state()}

  @callback handle_message(
              partition :: Brodex.partition(),
              message_or_message_set :: Brodex.Message.record() | Brodex.MessageSet.record(),
              callback_state :: callback_state
            ) :: {:ok, callback_state} | {:ok, :ack, callback_state}

  @typedoc """
  A spec for callback.

  `module` must implement `Brodex.TopicSubscriber` behaviour.
  """
  @type callback_spec :: module | {module, any}

  @typedoc "`t::brod_topic_subscriber.cb_state/0`"
  @type callback_state :: term

  @typedoc "`t::brod_topic_subscriber.committed_offsets/0`"
  @type committed_offsets :: [{Brodex.partition(), Brodex.offset()}]

  @type start_link_option ::
          {:client, Brodex.client()}
          | {:partition, :all | [Brodex.partition()]}
          | {:consumer_config, Brodex.consumer_config()}
          | {:message_type, :message | :message_set}

  @doc """
  Wrapper of `:brod_topic_subscriber.start_link/7`.
  """
  @spec start_link(Brodex.client(), Brodex.topic(), callback_spec, [start_link_option]) ::
          {:ok, pid} | {:error, term}
  def start_link(client, topic, callback_spec, options \\ [])

  def start_link(client, topic, {mod, args}, options) when is_atom(mod) do
    brod_args =
      [
        client,
        topic,
        Keyword.get(options, :partition, :all),
        Keyword.get(options, :consumer_config, [])
      ]
      |> add_arg(options, :committed_offset)
      |> add_arg(options, :message_type)

    brod_args = brod_args ++ [mod, args]

    apply(:brod_topic_subscriber, :start_link, brod_args)
  end

  def start_link(client, topic, mod, options) when is_atom(mod),
    do: start_link(client, topic, {mod, []}, options)

  defp add_arg(args, options, key) do
    if Keyword.has_key?(options, key) do
      args ++ [Keyword.get(options, key)]
    else
      args
    end
  end

  @doc """
  Wrapper of `:brod_topic_subscriber.stop/1`.
  """
  @spec stop(pid) :: :ok
  def stop(pid), do: :brod_topic_subscriber.stop(pid)
end
