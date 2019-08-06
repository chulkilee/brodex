defmodule Brodex do
  @moduledoc """
  Brodex is a thin wrapper of [`:brod`](https://hex.pm/packages/brod).

  ## Configuration

  See [brod README](https://github.com/klarna/brod) for details.

  ```elixir
  config :brod,
    clients: [
      my_client: [
        endpoints: [{'127.0.0.1', 9092}],
        reconnect_cool_down_seconds: 10
      ]
    ]
  ```

  If you use [mix release](https://hexdocs.pm/mix/Mix.Tasks.Release.html)

  ```elixir
  # config/releases.exs
  config :brod,
    clients: [
      my_client: [
        endpoints: Brodex.parse_endpoints(System.fetch_env!("KAFKA_ENDPOINTS)),
        reconnect_cool_down_seconds: 10
      ]
    ]
  ```
  """

  @typedoc "[`:brod.endpoint`](https://hexdocs.pm/brod/brod.html#type-endpoint)"
  @type endpoint :: {binary() | :inet.hostname(), non_neg_integer}

  @typedoc "[`:brod.client_id`](https://hexdocs.pm/brod/brod.html#type-client_id)"
  @type client_id :: atom

  @typedoc "[`:brod.client`](https://hexdocs.pm/brod/brod.html#type-client)"
  @type client :: client_id | pid

  @typedoc "[`:brod.client_config`](https://hexdocs.pm/brod/brod.html#type-client_config)"
  @type client_config :: :proplists.proplist()

  @typedoc "[`:brod.connection`](https://hexdocs.pm/brod/brod.html#type-connection)"
  @type connection :: pid

  @typedoc "[`:brod.bootstrap`](https://hexdocs.pm/brod/brod.html#type-bootstrap)"
  @type bootstrap :: [endpoint] | {[endpoint], client_config}

  @typedoc "[`:brod.topic`](https://hexdocs.pm/brod/brod.html#type-topic)"
  @type topic :: binary

  @typedoc "[`:brod.partition`](https://hexdocs.pm/brod/brod.html#type-partition)"
  @type partition :: int32

  @typedoc "[`:brod.key`](https://hexdocs.pm/brod/brod.html#type-key)"
  @type key :: :undefined | binary

  @typedoc "[`:brod.offset`](https://hexdocs.pm/brod/brod.html#type-offset)"
  @type offset :: int64

  @typedoc "[`:brod.msg_ts`](https://hexdocs.pm/brod/brod.html#type-msg_ts)"
  @type msg_ts :: int64

  @typedoc "[`:brod.call_ref`](https://hexdocs.pm/brod/brod.html#type-call_ref)"
  @type call_ref ::
          {:brod_call_ref, caller :: :undefined | pid, callee :: :undefined | pid,
           ref :: :undefined | reference}

  @typedoc "[`:brod_producer.config`](https://hexdocs.pm/brod/brod_producer.html#type-config)"
  @type producer_config :: :proplists.proplist()

  @typedoc "[`:brod_consumer.config`](https://hexdocs.pm/brod/brod_consumer.html#type-config)"
  @type consumer_config :: :proplists.proplist()

  @typedoc "[`:brod.value`](https://hexdocs.pm/brod/brod.html#type-value)"
  @type value ::
          :undefined
          | iodata
          | {msg_ts, binary}
          | [{key, value}]
          | [{msg_ts, key, value}]
          | msg_input
          | batch_input

  @typedoc "[`:kpro.headers`](https://hexdocs.pm/kafka_protocol/kpro.html#type-headers)"
  @type headers :: [{binary, binary}]

  @typedoc "[`:kpro.msg_input`](https://hexdocs.pm/kafka_protocol/kpro.html#type-msg_input)"
  @type msg_input :: %{headers: headers, ts: msg_ts, key: key, value: value}

  @typedoc "[`:kpro.batch_input`](https://hexdocs.pm/kafka_protocol/kpro.html#type-batch_input)"
  @type batch_input :: [msg_input]

  @typedoc "[`:brod.partitioner`](https://hexdocs.pm/brod/brod.html#type-partitioner)"
  @type partitioner :: (topic, pos_integer, key, value -> {:ok, partition}) | :random | :hash

  @typedoc "[`:brod.group_id`](https://hexdocs.pm/brod/brod.html#type-group_id)"
  @type group_id :: binary

  @typedoc "[`:brod.group_config`](https://hexdocs.pm/brod/brod.html#type-group_config)"
  @type group_config :: :proplists.proplist()

  @typedoc "[`:kpro.int32`](https://hexdocs.pm/kafka_protocol/kpro.html#type-int32)"
  @type int32 :: -2_147_483_648..2_147_483_647

  @typedoc "[`:kpro.int64`](https://hexdocs.pm/kafka_protocol/kpro.html#type-int64)"
  @type int64 :: -9_223_372_036_854_775_808..9_223_372_036_854_775_807

  @doc """
  Wrapper of [`:brod.start_client/3`](https://hexdocs.pm/brod/brod.html#start_client-3).
  """
  @spec start_client([endpoint], client_id, :brod.client_config()) ::
          :ok | {:error, term}
  def start_client(endpoints, client_id, options \\ []),
    do: :brod.start_client(endpoints, client_id, options)

  @doc """
  Wrapper of [`:brod.stop_client/1`](https://hexdocs.pm/brod/brod.html#stop_client-1).
  """
  @spec stop_client(client) :: :ok
  def stop_client(client),
    do: :brod.stop_client(client)

  @doc """
  Wrapper of [`:brod.start_consumer/3`](https://hexdocs.pm/brod/brod.html#start_consumer-3).
  """
  @spec start_consumer(client, topic, consumer_config) :: :ok
  def start_consumer(client, topic, consumer_config \\ []),
    do: :brod.start_consumer(client, topic, consumer_config)

  @doc """
  Wrapper of [`:brod.start_producer/3`](https://hexdocs.pm/brod/brod.html#start_producer-3).
  """
  @spec start_producer(client, topic, producer_config) :: :ok
  def start_producer(client, topic, producer_config \\ []),
    do: :brod.start_producer(client, topic, producer_config)

  @doc """
  Wrapper of [`:brod.produce/5`](https://hexdocs.pm/brod/brod.html#produce-5).
  """
  @spec produce_async(client, topic, input :: {key, value} | value, partition | partitioner) ::
          {:ok, call_ref} | {:error, term}
  def produce_async(client, topic, input, partition_spec \\ :hash)

  def produce_async(client, topic, value, partition_spec) when is_list(value) or is_map(value),
    do: :brod.produce(client, topic, partition_spec, "", value)

  def produce_async(client, topic, {key, value}, partition_spec),
    do: :brod.produce(client, topic, partition_spec, key, value)

  @doc """
  Wrapper of [`:brod.produce_sync/5`](https://hexdocs.pm/brod/brod.html#produce_sync-5).
  """
  @spec produce_sync(client, topic, input :: {key, value} | value, partition | partitioner) ::
          :ok | {:error, term}
  def produce_sync(client, topic, input, partition_spec \\ :hash)

  def produce_sync(client, topic, value, partition_spec) when is_list(value) or is_map(value),
    do: :brod.produce_sync(client, topic, partition_spec, "", value)

  def produce_sync(client, topic, {key, value}, partition_spec),
    do: :brod.produce_sync(client, topic, partition_spec, key, value)

  @doc """
  Wrapper of [`:brod.get_metadata/3`](https://hexdocs.pm/brod/brod.html#get_metadata-3)
  """
  @spec get_metadata([endpoint], :all | [topic], :brod.conn_config()) ::
          {:ok, :kpro.struct()} | {:error, term}
  def get_metadata(endpoints, topic, connect_config \\ []),
    do: :brod.get_metadata(endpoints, topic, connect_config)

  @doc """
  Wrapper of [`:brod.get_partitions_count/2`](https://hexdocs.pm/brod/brod.html#get_partitions_count-2)
  """
  @spec get_partitions_count(client, topic) :: {:ok, pos_integer} | {:error, term}
  def get_partitions_count(client, topic), do: :brod.get_partitions_count(client, topic)

  @doc """
  Wrapper of [`:brod.list_all_groups/2`](https://hexdocs.pm/brod/brod.html#list_all_groups-2)
  """
  @spec list_all_consumer_groups([endpoint], :brod.conn_config()) :: [
          {endpoint, [:brod.cg()] | {:error, term}}
        ]
  def list_all_consumer_groups(endpoints, connect_config \\ []),
    do: :brod.list_all_groups(endpoints, connect_config)

  @doc """
  Wrapper of [`:brod.list_groups/2`](https://hexdocs.pm/brod/brod.html#list_groups-2)
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
  Wrapper of [`:brod.fetch/5`](https://hexdocs.pm/brod/brod.html#fetch-5)
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
