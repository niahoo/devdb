defprotocol DevDB.Store do
  def put_entry(state, entry)
  def delete_entry(state, key)
  def fetch_entry(state, key)
  def reduce_entries(state, acc, fun)
  def reduce_tr_entries(state, ref, acc, fun)
end
