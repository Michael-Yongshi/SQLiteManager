import logging

from sqlitemanager.database import Database
from sqlitemanager import handler


# connect to new database
db = Database(filename="science")
db.close()

# connect to existing database
db = Database(filename="science")

# create a table
table_dict_gender = {"gender": {
        "id": {"column_type": "int", "primary_key": True, "autonumber": True},
        "name": {"column_type": "str",},
    }}
handler.create_table(db=db, config_dict=table_dict_gender)

# create a table with records
table_dict_sex = {"sex": {
        "id": {"column_type": "int", "primary_key": True, "autonumber": True},
        "name": {"column_type": "str",},
    }}
records_sex = {"sex":[
        {"name":"Male"},
        {"name":"Female"}
        ]
    }
handler.create_table(db=db, config_dict=table_dict_sex, record_dict=records_sex)

# create a table with a foreign key to the first table
table_dict_scientists = {"scientist": {
        "id": {"column_type": "int", "primary_key": True, "autonumber": True},
        "name": {"column_type": "str"},
        "age": {"column_type": "int"},
        "sex_id": {"column_type": "int", "foreign_key": {"sex":"id"}},
    }}
handler.create_table(db=db, config_dict=table_dict_scientists)


# add some records directly in the database
handler.create_records(db=db, table_name="scientist",
    records=[
        {"name":"Hawking", "age":68, "sex_id":1},
        {"name":"Marie Curie", "age":20, "sex_id":2},
        {"name":"Einstein", "age":100, "sex_id":1},
    ])

# test deleting of table
handler.delete_table(db=db, table_name="gender")

# get all table names
print(f"table names are {handler.get_table_names(db=db)}")

# get metadata info and check single table fetching
metadata_string = handler.get_table_metadata(db=db, table_selection="scientist")
metadata_dict_of_one = handler.get_table_metadata(db=db, table_selection=["scientist"])
if metadata_string != metadata_dict_of_one["scientist"]:
    logging.warning(f"get_table_metadata does not accept both single table as list or single table as string selection")
print(f"table metadata is {handler.get_table_metadata(db=db)}")

# get table column names and check single table fetching
column_names_string = handler.get_table_column_names(db=db, table_selection="scientist")
column_names_dict_of_one = handler.get_table_column_names(db=db, table_selection=["scientist"])["scientist"]
if column_names_string != column_names_dict_of_one:
    logging.warning(f"get_table_column_names does not accept both single table as list or single table as string selection")
print(handler.get_table_column_names(db=db))

# get tables and check single table fetching
table_string = handler.get_tables(db=db, table_selection="scientist")
table_dict_of_one = handler.get_tables(db=db, table_selection=["scientist"])["scientist"]
if type(table_string) != type(table_dict_of_one):
    logging.warning(f"get_tables does not accept both single table as list or single table as string selection")
all_tables = handler.get_tables(db=db)
print(f"table objects in database are {all_tables}")

# get table object with records
table_sex = handler.get_tables(db=db, table_selection="sex")
table_scientist = handler.get_tables(db=db, table_selection="scientist")

# print records of table
print(table_scientist.records)
# def print_records(records):
    
#     for record in records:
#         record.print()

# print_records(records=table_scientist.records)

# get table object
# print(handler.get_tables(db=db, table_selection="scientist"))














# delete database
db.delete()




# # adding more tables with column dict
# table_nobel = handler.table_create(
#     table_name = "nobelprizes",
#     column_dict={
#         "name": "string",
#         "description": "str",
#     },
# )


# # add some records directly in the database with recorddict
# handler.table_create_add_records(
#     table_name="scientists",
#     recorddicts=[
#         {"name":"Rosenburg", "age": 78, "gender_id": 1,},
#         {"name":"Neil dGrasse Tyson", "age": 57, "gender_id": True,},
#         ]
#     )

# # gather records through accessing the table object (better performance)
# print(handler.database.tables["scientists"].records)

# # read the records from the database (performance heavy for large databases)
# print(handler.table_read_records(table_name="scientists"))

# # conditional read with where statement
# where = [["gender_id", [1]]]
# records = handler.table_read_records(table_name="scientists", where=where)
# print(f"read where")
# print_records(records)

# # update with where statement
# valuepairs = [["gender_id", 1]]
# where = [["name", ["Hawking"]]]
# records = handler.table_update_records(table_name="scientists", valuepairs=valuepairs, where=where)
# print(f"update true to false")
# print_records(records)

# # update with direct primary id
# valuepairs = [["name", "Neil de'Grasse Tyson"], ["age", 40]]
# primarykey = 5
# records = handler.table_update_records(table_name="scientists", valuepairs=valuepairs, where=primarykey)
# print(f"update record 'id = 5'")
# print_records(records)

# table_papers = handler.table_create(
#     table_name = "papers",
#     column_dict={
#         "name": "string",
#         "description": "str",
#     },
# )
# table_nobel = handler.table_create(
#     table_name = "ignorants",
#     column_dict={
#         "name": "string",
#         "description": "str",
#     },
# )

# # creating multiple records and return them (these are not yet saved in the database)
# records = handler.records_create(
#     table_name="nobelprizes",
#     recordsvalues=[
#         ["Peace", "Peace nobel prize"],
#         ["Economy", "Economy nobel prize"],
#         ["Physics", "Physics nobel prize"],
#         ["Sociology", "Sociology nobel prize"],
#         ]
#     )
# # adding these records to the database
# records = handler.table_add_records(table_name="nobelprizes", records=records)
# print(f"creating multiple records")
# print_records(records)

# # again
# records = handler.table_create_add_records(
#     table_name="papers",
#     recordsvalues=[
#         ["Palestine", "Extrapolation on the palestinian cause"],
#         ["Wealth", "On the Wealth of Nations"],
#         ["Time", "A brief history of time"],
#         ["Fear", "Controlling your fear"],
#         ]
#     )
# print(f"creating multiple records")
# print_records(records)

# # adding a crossref table
# crossref_table = handler.crossref_create(
#     table_name1="scientists",
#     table_name2="nobelprizes",
# )
# print(f"Database contains {handler.database.tables}")

# # adding a crossref table
# crossref_table = handler.crossref_create(
#     table_name1="scientists",
#     table_name2="papers",
# )
# print(f"Database contains {handler.database.tables}")

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
