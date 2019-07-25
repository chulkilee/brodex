defmodule Brodex.Guards do
  @doc """
  Return `true`if `term` is `t:Brodex.Message.record/0`.

  ## Examples

      iex>  Brodex.Guards.is_message_record({:kafka_message, 164, "", "hello", :create, 1_563_946_803_056, []})
      true


      iex>  Brodex.Guards.is_message_record(
      ...>    {:kafka_message_set, "my_topic", 0, 33,
      ...>    [
      ...>      {:kafka_message, 31, "", "a", :create, 1_564_023_091_657, []},
      ...>      {:kafka_message, 32, "", "b", :create, 1_564_023_091_894, []}
      ...>    ]}
      ...>  )
      false

  """
  defguard is_message_record(term)
           when is_tuple(term) and tuple_size(term) == 7 and elem(term, 0) == :kafka_message

  @doc """
  Return `true`if `term` is `t:Brodex.MessageSet.record/0`.

  ## Examples

      iex>  Brodex.Guards.is_message_set_record(
      ...>    {:kafka_message_set, "my_topic", 0, 33,
      ...>    [
      ...>      {:kafka_message, 31, "", "a", :create, 1_564_023_091_657, []},
      ...>      {:kafka_message, 32, "", "b", :create, 1_564_023_091_894, []}
      ...>    ]}
      ...>  )
      true

      iex>  Brodex.Guards.is_message_set_record({:kafka_message, 164, "", "hello", :create, 1_563_946_803_056, []})
      false
  """
  defguard is_message_set_record(term)
           when is_tuple(term) and tuple_size(term) == 5 and elem(term, 0) == :kafka_message_set
end
