defmodule Brodex.GroupSubscriber do
  @moduledoc """
  Wrapper of`m::brod_group_subscriber`.
  """

  @callback init(group_id :: Brodex.group_id(), term) ::
              {:ok, callback_state}

  @callback handle_message(
              topic :: Brodex.topic(),
              partition :: Brodex.partition(),
              message_or_message_set :: Brodex.Message.record() | Brodex.MessageSet.record(),
              callback_state :: callback_state
            ) ::
              {:ok, callback_state}
              | {:ok, :ack, callback_state}
              | {:ok, :ack_no_commit, callback_state}

  @typedoc """
  A spec for callback.

  `module` must implement `Brodex.GroupSubscriber` behaviour.
  """
  @type callback_spec :: module | {module, any}

  @typedoc "`t::brod_group_subscriber.cb_state/0`"
  @type callback_state :: term

  @type start_link_option ::
          {:group_config, Brodex.group_config()}
          | {:consumer_config, Brodex.consumer_config()}
          | {:message_type, :message | :message_set}

  @doc """
  Wrapper of `:brod_group_subscriber.start_link/8`.
  """
  @spec start_link(Brodex.client(), Brodex.group_id(), [Brodex.topic()], callback_spec, [
          start_link_option
        ]) ::
          {:ok, pid} | {:error, term}
  def start_link(client, group_id, topics, callback_spec, options)

  def start_link(client, group_id, topics, {mod, args}, options) do
    :brod_group_subscriber.start_link(
      client,
      group_id,
      topics,
      Keyword.get(options, :group_config, []),
      Keyword.get(options, :consumer_config, []),
      Keyword.get(options, :message_type, :message),
      mod,
      args
    )
  end

  def start_link(client, group_id, topics, mod, options) when is_atom(mod),
    do: start_link(client, group_id, topics, {mod, []}, options)

  @doc """
  Wrapper of `:brod_group_subscriber.stop/1`.
  """
  @spec stop(pid) :: :ok
  def stop(pid), do: :brod_group_subscriber.stop(pid)
end
