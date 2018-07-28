defmodule DevDB.Repository.Entry do
  require Record
  use TODO
  @todo "Remove ref entry as it is used in values ? But it's useful to match everything at once"
  Record.defrecord(:db_entry, key: nil, value: nil, trref: nil, trval: nil, trinserted: false)

  def match_spec_base() do
    db_entry(key: :_, value: :_, trref: :_, trval: :_, trinserted: :_)
  end

  # If in a transaction we delete a record, this value will be put in the :trval
  # field, showing that this record should be ignored
  def deleted_value(ref), do: {ref, :deleted_value}

  # In case we operate a dirty read on a table where a transaction is in
  # progress, and in this transaction we inserted a value (in the :trval field),
  # the :value field of the record has not an actual value yet, so it will be
  # set to @inserted_value.
  def inserted_value(ref), do: {ref, :inserted_value}

  # Indicates that a new value has been inserted for this record in this
  # transaction
  def updated_value(value, ref), do: {ref, :updated_value, value}
end
