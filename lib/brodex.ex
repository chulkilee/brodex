defmodule Brodex do
  @moduledoc """
  Brodex is a thin wrapper of `m::brod`.

  ## Configuration

  See [brod README](https://github.com/kafka4beam/brod) for details.

  ```elixir
  config :brod,
    clients: [
      my_client: [
        endpoints: [{'127.0.0.1', 9092}],
        auto_start_producers: true,
        reconnect_cool_down_seconds: 10
      ]
    ]
  ```

  ```elixir
  # config/runtime.exs
  config :brod,
    clients: [
      my_client: [
        endpoints: Brodex.parse_endpoints(System.fetch_env!("KAFKA_ENDPOINTS")),
        auto_start_producers: true,
        reconnect_cool_down_seconds: 10
      ]
    ]
  ```
  """

  @typedoc "`t::brod.endpoint/0`"
  @type endpoint :: {binary() | :inet.hostname(), non_neg_integer}

  @typedoc "`t::brod.client_id/0`"
  @type client_id :: atom

  @typedoc "`t::brod.client/0`"
  @type client :: client_id | pid

  @typedoc "`t::brod.client_config/0`"
  @type client_config :: :proplists.proplist()

  @typedoc "`t::brod.connection/0`"
  @type connection :: pid

  @typedoc "`t:brod.bootstrap/0`"
  @type bootstrap :: [endpoint] | {[endpoint], client_config}

  @typedoc "`t:brod.topic/0`"
  @type topic :: binary

  @typedoc "`t:brod.partition/0`"
  @type partition :: int32

  @typedoc "`t:brod.key/0`"
  @type key :: :undefined | binary

  @typedoc "`t:brod.offset/0`"
  @type offset :: int64

  @typedoc "`t:brod.msg_ts/0`"
  @type msg_ts :: int64

  @typedoc "`t:brod.call_ref/0`"
  @type call_ref ::
          {:brod_call_ref, caller :: :undefined | pid, callee :: :undefined | pid,
           ref :: :undefined | reference}

  @typedoc "`t::brod_producer.config/0`"
  @type producer_config :: :proplists.proplist()

  @typedoc "`t::brod_consumer.config/0`"
  @type consumer_config :: :proplists.proplist()

  @typedoc "`t::brod.value/0`"
  @type value ::
          :undefined
          | iodata
          | {msg_ts, binary}
          | [{key, value}]
          | [{msg_ts, key, value}]
          | msg_input
          | batch_input

  @typedoc "`t::kpro.headers/0`"
  @type headers :: [{binary, binary}]

  @typedoc "`t::kpro.msg_input/0`"
  @type msg_input :: %{headers: headers, ts: msg_ts, key: key, value: value}

  @typedoc "`t::kpro.batch_input/0`"
  @type batch_input :: [msg_input]

  @typedoc "`t::brod.partitioner/0`"
  @type partitioner :: (topic, pos_integer, key, value -> {:ok, partition}) | :random | :hash

  @typedoc "`t::brod.group_id/0`"
  @type group_id :: binary

  @typedoc "`t::brod.group_config/0`"
  @type group_config :: :proplists.proplist()

  @typedoc "`t::kpro.int32/0`"
  @type int32 :: -2_147_483_648..2_147_483_647

  @typedoc "`t::kpro.int64/0`"
  @type int64 :: -9_223_372_036_854_775_808..9_223_372_036_854_775_807

  @doc """
  Wrapper of `:brod.start_client/3`.
  """
  @spec start_client([endpoint], client_id, :brod.client_config()) ::
          :ok | {:error, term}
  def start_client(endpoints, client_id, options \\ []),
    do: :brod.start_client(endpoints, client_id, options)

  @doc """
  Wrapper of `:brod.stop_client/1`.
  """
  @spec stop_client(client) :: :ok
  def stop_client(client),
    do: :brod.stop_client(client)

  @doc """
  Wrapper of `:brod.start_consumer/3`.
  """
  @spec start_consumer(client, topic, consumer_config) :: :ok
  def start_consumer(client, topic, consumer_config \\ []),
    do: :brod.start_consumer(client, topic, consumer_config)

  @doc """
  Wrapper of `:brod.start_producer/3`.
  """
  @spec start_producer(client, topic, producer_config) :: :ok
  def start_producer(client, topic, producer_config \\ []),
    do: :brod.start_producer(client, topic, producer_config)

  @doc """
  Wrapper of `:brod.produce/5`.
  """
  @spec produce_async(client, topic, input :: {key, value} | value, partition | partitioner) ::
          {:ok, call_ref} | {:error, term}
  def produce_async(client, topic, input, partition_spec \\ :hash)

  def produce_async(client, topic, value, partition_spec) when is_list(value) or is_map(value),
    do: :brod.produce(client, topic, partition_spec, "", value)

  def produce_async(client, topic, {key, value}, partition_spec),
    do: :brod.produce(client, topic, partition_spec, key, value)

  @doc """
  Wrapper of `:brod.produce_sync/5`.
  """
  @spec produce_sync(client, topic, input :: {key, value} | value, partition | partitioner) ::
          :ok | {:error, term}
  def produce_sync(client, topic, input, partition_spec \\ :hash)

  def produce_sync(client, topic, value, partition_spec) when is_list(value) or is_map(value),
    do: :brod.produce_sync(client, topic, partition_spec, "", value)

  def produce_sync(client, topic, {key, value}, partition_spec),
    do: :brod.produce_sync(client, topic, partition_spec, key, value)

  @doc """
  Wrapper of `:brod.get_metadata/3`.
  """
  @spec get_metadata([endpoint], :all | [topic], :brod.conn_config()) ::
          {:ok, :kpro.struct()} | {:error, term}
  def get_metadata(endpoints, topic, connect_config \\ []),
    do: :brod.get_metadata(endpoints, topic, connect_config)

  @doc """
  Wrapper of `:brod.get_partitions_count/2`.
  """
  @spec get_partitions_count(client, topic) :: {:ok, pos_integer} | {:error, term}
  def get_partitions_count(client, topic), do: :brod.get_partitions_count(client, topic)

  @doc """
  Wrapper of `:brod.list_all_groups/2`.
  """
  @spec list_all_consumer_groups([endpoint], :brod.conn_config()) :: [
          {endpoint, [:brod.cg()] | {:error, term}}
        ]
  def list_all_consumer_groups(endpoints, connect_config \\ []),
    do: :brod.list_all_groups(endpoints, connect_config)

  @doc """
  Wrapper of `:brod.list_groups/2`.
  """
  @spec list_consumer_groups(endpoint, :brod.conn_config()) ::
          {:ok, [:brod.cg()]} | {:error, term}

  def list_consumer_groups(endpoint, connect_config \\ []),
    do: :brod.list_groups(endpoint, connect_config)

  @type fetch_opt ::
          {:max_wait_time, non_neg_integer}
          | {:min_bytes, non_neg_integer}
          | {:max_bytes, non_neg_integer}
  @doc """
  Wrapper of `:brod.fetch/5`.
  """
  @spec fetch(
          connection | client_id | bootstrap,
          topic,
          partition,
          offset,
          [fetch_opt]
        ) ::
          {:ok, {offset, [Brodex.Message.record()]}} | {:error, term}
  def fetch(conn_spec, topic, partition, offset, options \\ []),
    do: :brod.fetch(conn_spec, topic, partition, offset, Enum.into(options, %{}))

  @doc """
  Parse endpoints.

  ## Examples

      iex> Brodex.parse_endpoints("kafka1:9000,kafka:9001")
      [{'kafka1', 9000}, {'kafka', 9001}]

  """
  @spec parse_endpoints(String.t()) :: [endpoint]
  def parse_endpoints(endpoints)

  def parse_endpoints(endpoints) when is_binary(endpoints) do
    endpoints
    |> String.split(",")
    |> Enum.map(fn host_port ->
      [host, port] = String.split(host_port, ":")
      {port, ""} = Integer.parse(port)
      {to_charlist(host), port}
    end)
  end
end
