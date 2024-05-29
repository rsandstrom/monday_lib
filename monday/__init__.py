"""
  ***********************************************
  The Monday class requires a board id and either a monday token or Monday account (email) to access a board.

  The optional unique_key field is used to build a key map where we can lookup to see if a record already exists.

  to use the unique_key to prevent duplicates, pass the column names in a list

      Example: key = Key.unique(group_name=True, row_name=True, field_names=['Numbers', 'Text Key'])

  If the key is used, the monday class will create a key for each row and
  add the key to the key_map dict for quick lookup.
  If the unique_key constraint is violated and exception will be raised

  To load a monday board using the monday account:

       Example: demo_site = Monday(2154004550, unique_key=key, monday_account='automation@com')

  To load a monday board using the monday token:

       Example: demo_site = Monday(2154004550, monday_token, unique_key=key)

  ***********************************************
"""