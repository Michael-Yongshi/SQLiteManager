import logging

import os

from sqlitemanager.objects import Database, Table, Record
from sqlitemanager import handler


def print_records(records, description=""):

    if description != "":
        print(description)

    for record in records:
        record.print()

    print()

# connect to new database in custom path
path = os.path.join(os.getcwd(),"test_directory")
db = Database(path=path, filename="custompath", extension=("sqlite"))
db.delete()

# enforce a clean database for testing
db = Database(filename="science")
db.delete()

# connect to new database in current directory
db = Database(filename="science")
db.close()

# connect to existing database
db = Database(filename="science")



# create a table
table_dict = {"gender": {
        "id": {"column_type": "int", "primary_key": True, "autonumber": True},
        "name": {"column_type": "txt",},
    }}
handler.create_table(db=db, config_dict=table_dict)

# create a table with records
table_dict = {"sex": {
        "id": {"column_type": "number", "primary_key": True, "autonumber": True},
        "name": {"column_type": "TEXT",},
    }}
record_dict = {"sex":[
        {"name":"Male"},
        {"name":"Female"}
        ]
    }
handler.create_table(db=db, config_dict=table_dict, record_dict=record_dict)

# create a table with a foreign key to the first table
table_dict = {
    "scientist": {
        "id": {"column_type": "INTEGER", "primary_key": True, "autonumber": True},
        "name": {"column_type": "str"},
        "age": {"column_type": "#"},
        "sex_id": {"column_type": "INT", "foreign_key": {"sex":"id"}},
        "creation_date": {"column_type": "dt", "column_default":"now"},
    },
    "nobelprize": {
        "id": {"column_type": "number", "primary_key": True, "autonumber": True},
        "name": {"column_type": "TEXT",},
        "description": {"column_type": "string",},
    }}
record_dict = {
    "nobelprize":[
        {"name":"Peace", "description":"Peace nobel prize"},
        {"name":"Economy", "description":"Economy nobel prize"},
        {"name":"Physics", "description":"Physics nobel prize"},
        {"name":"Sociology", "description":"Sociology nobel prize"},
    ]}
handler.create_table(db=db, config_dict=table_dict, record_dict=record_dict)




# test deleting of table
handler.delete_table(db=db, table_name="gender")




# add some records with a list of dicts, columname: value
handler.create_records(db=db, table_name="scientist",
    records=[
        {"name":"Hawking", "age":68, "sex_id":1},
        {"name":"Mary Curie", "age":20, "sex_id":2},
        {"name":"Einstein", "age":100, "sex_id":True},
        {"name":"Rosenburg", "age":78, "sex_id":1},
        {"name":"Neil dGrasse Tyson", "age":57, "sex_id":1},
    ])



# get all table names
print(f"table names are {handler.get_table_names(db=db)}\n")

# get metadata info and check single table fetching
metadata_string = handler.get_table_metadata(db=db, table_selection="scientist")
metadata_dict_of_one = handler.get_table_metadata(db=db, table_selection=["scientist"])
if metadata_string != metadata_dict_of_one["scientist"]:
    logging.warning(f"get_table_metadata does not accept both single table as list or single table as string selection")
print(f"table metadata is {handler.get_table_metadata(db=db)}\n")

# get table column names and check single table fetching
column_names_string = handler.get_table_column_names(db=db, table_selection="scientist")
column_names_dict_of_one = handler.get_table_column_names(db=db, table_selection=["scientist"])["scientist"]
if column_names_string != column_names_dict_of_one:
    logging.warning(f"get_table_column_names does not accept both single table as list or single table as string selection")
print(f"{handler.get_table_column_names(db=db)}\n")



# get tables and check single table fetching
table_string = handler.get_tables(db=db, table_selection="scientist")
table_dict_of_one = handler.get_tables(db=db, table_selection=["scientist"])["scientist"]
if type(table_string) != type(table_dict_of_one):
    logging.warning(f"get_tables does not accept both single table as list or single table as string selection")
all_tables = handler.get_tables(db=db)
print(f"table objects in database are {all_tables}\n")

# get table object with records
table_sex = handler.get_tables(db=db, table_selection="sex")
print_records(table_sex.records, description="Table Sex records")
table_scientist = handler.get_tables(db=db, table_selection="scientist")
print_records(table_scientist.records, description="Table Scientist records")





# get records directly from database with where examples
where = {"sex_id":{
    "operator":"!=",
    "values":1}}
records = handler.get_records(db=db, table_name="scientist", columns=[], where=where)
print_records(records, description="get female scientists")

where = {"age":{
    "operator":">",
    "values":50}}
records = handler.get_records(db=db, table_name="scientist", columns=[], where=where)
print_records(records, description="get scientists over 50 years old")

where = {"name":{
    "operator":"IN",
    "values":["Einstein", "Mary Curie"]}}
records = handler.get_records(db=db, table_name="scientist", columns=[], where=where)
print_records(records, description="get scientists named Einstein and Marie Curie")

where = {"age":{
    "operator":"IN",
    "values":[68,20]}}
records = handler.get_records(db=db, table_name="scientist", columns=[], where=where)
print_records(records, description="get scientists aged 68 and 20")

# get max row (latest record)
record = handler.get_latest_record(db=db, table_name="scientist", column_name="id")
print(f"Getting the latest record in a table")
print(f"{record}\n")



# update record through Record object with changes outside the database
where = {"name":{
    "operator":"=",
    "values":"Einstein"}}
record = handler.get_record(db=db, table_name="scientist", where=where)
record.dict.update({"age": 76})
handler.update_record_by_difference(db=db, record_object=record)
print_records(records=[record], description="Updated Einstein to age 76 with object")

# update records directly
where = {"name":{
    "operator":"=",
    "values":"Mary Curie"}}
update = {"name":"Marie Curie"}
handler.update_records(db=db, table_name="scientist", update_dict=update, where=where)
where = {"name":{
    "operator":"=",
    "values":"Marie Curie"}}
record = handler.get_record(db=db, table_name="scientist", where=where)
print_records(records=[record], description="Updated Mary to Marie")





# printing records
print("Marie Curie test print record contents")
print(f"table is {record.table} with columns {record.columns}")
print(f"values are {record.values}")
print(f"value dict is {record.dict}")
print()



# update record based on edited record object / dataclass
# handler.compare_and_update_record(db=db, record=record)
# would be handy



# delete records
where = {"name":{
    "operator":"=",
    "values":"Rosenburg"}}
handler.delete_records(db=db, table_name="scientist", where=where)
print(f"Deleted record of Rosenburg")
handler.get_records(db=db, table_name="scientist", where=where)
print()




# adding a crossref table
xref_table_dict = {
    "scientist":"id",
    "nobelprize":"id",
    }
handler.create_xref_table(db=db, xref_dict = xref_table_dict)

# cross reference example one to many
xref_record_dict = {
    "scientist": {
        "name":{
        "operator":"=",
        "values":["Einstein"]
        }},
    "nobelprize": {
        "name":{
        "operator":"in",
        "values":["Physics", "Peace"]
        }}}
handler.create_xref_records(db=db, xref_record_dict = xref_record_dict)

# cross reference example many to many
xref_record_dict = {
    "scientist": {
        "name":{
        "operator":"in",
        "values":["Hawking", "Marie Curie"]
        }},
    "nobelprize": {
        "name":{
        "operator":"in",
        "values":["Economy", "Sociology"]
        }}}
handler.create_xref_records(db=db, xref_record_dict = xref_record_dict)

# get xref records for a single record in source table
where = {"name":{
    "operator":"=",
    "values":["Einstein"]}}
handler.get_xref_records(db=db, source_table="scientist", target_table="nobelprize", source_where=where)

# get many to many xref combinations
where = {"name":{
    "operator":"in",
    "values":["Einstein","Marie Curie"]}}
handler.get_xref_records(db=db, source_table="scientist", target_table="nobelprize", source_where=where)

# delete database
db.delete()


# # getting a crossref table
# crossref_table = handler.crossref_get(
#     table_name1="scientists",
#     table_name2="papers",
# )
# print(f"Crossref table is {crossref_table}")

# # get crossreferences
# crossreferences = handler.crossref_get_all(table_name="scientists")
# print(f"Table scientists has links to:")
# for crossreference in crossreferences:
#     print(f"- {crossreference.name}")

# # adding a crossref
# handler.crossref_add_record(
#     table_name1="scientists",
#     table_name2="nobelprizes",
#     where1=[
#         ["name", ["Hawking"]]
#         ],
#     where2=[
#         ["name", ["Economy"]]
#         ],
# )

# # adding a crossref
# handler.crossref_add_record(
#     table_name1="scientists",
#     table_name2="nobelprizes",
#     where1=[
#         ["name", ["Einstein"]]
#         ],
#     where2=[
#         ["name", ["Physics", "Peace"]]
#         ],
#     description="Einstein wins both peace and physics nobel prizes"
# )

# # adding crossreff based upon primary keys
# handler.crossref_add_record(
#     table_name1="scientists",
#     table_name2="papers",
#     where1=[1],
#     where2=[3, 4],
#     description="Hawking wrote papers about time and fear of time"
# )

# # reading crossreferences of all scientists and nobelprizes
# records = handler.crossref_read_records(
#     table_name1="scientists",
#     table_name2="nobelprizes",
#     )
# print_records(records)

# # reading crossreferences of a single scientist and papers
# records = handler.crossref_read_record(
#     table_name1="scientists",
#     table_name2="papers",
#     primarykey=1,
# )
# print(f"looking up Hawkings papers:")
# print_records(records)

# # deleting record
# where = [
#     ["name", ["Rosenburg"]]
# ]
# handler.table_delete_records(table_name="scientists", where=where)
# print(f"deleted record rosenburg")
# records = handler.table_read_records(table_name="scientists")
# print_records(records)

# # delete a table
# handler.table_delete(table_name="ignorants")

# # gathering all records of all tables
# recordset = []
# for table_name in handler.database.tables:
#     records = handler.table_read_records(table_name)
#     recordset += [records]

# # printing all the records of all tables
# print(f"Database contains {handler.database.tables}")
# for records in recordset:
#     print("")
#     print_records(records)

# # clean up by closing and deleting the database
# dest_path = handler.path + "/science"
# dest_file = "science_copy"

# copy = handler.database_saveas(filename=dest_file, path=dest_path)
# handler.database_close()

# handler.filename=dest_file
# handler.path=dest_path
# handler.database_open()

# handler.check_database_info()
