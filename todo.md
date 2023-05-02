- change record objects then send in for update instead of manually arrange update calls.
  like,

  def update_record_difference(record_object, column_id):

    create where based on the column_id (column that idnetifies the record uniquely)
    look up original record
    compare lookup_object and record_object dicts
    if different add to update dict with the new value
    call update_record function with the update dict and the same where