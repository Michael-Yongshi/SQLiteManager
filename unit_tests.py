import logging

from sqlitemanager.database import Database
from sqlitemanager import handler


# connect to database
db = Database(filename="science")

table_dict_genders = {
    "genders": {
        "id": {
            "column_type": "int",
            "primary_key": True,
            "autonumber": True,
        },
        "name": {
            "column_type": "str",
        }
    }
}

handler.table_create(db=db, config_dict=table_dict_genders)

table_dict_scientists = {
    "scientists": {
        "id": {
            "column_type": "int",
            "primary_key": True,
            "autonumber": True,
        },
        "name": {
            "column_type": "str",
        },
        "age": {
            "column_type": "int",
        },
        "gender_id": {
            "column_type": "int",
            "foreign_key": {
                "genders":"id"
            }
        },
    }
}

handler.table_create(db=db, config_dict=table_dict_scientists)

db.delete()

# def print_records(records):
    
#     for record in records:
#         record.print()



# # adding more tables with column dict
# table_nobel = handler.table_create(
#     tablename = "nobelprizes",
#     column_dict={
#         "name": "string",
#         "description": "str",
#     },
# )

# # add some records directly in the database
# handler.table_create_add_records(
#     tablename="scientists",
#     recordsvalues=[
#         ["Hawking", 68, 2],
#         ["Marie Curie", 20, 2],
#         ["Einstein", 100, 1],
#         ]
#     )

# # add some records directly in the database with recorddict
# handler.table_create_add_records(
#     tablename="scientists",
#     recorddicts=[
#         {"name":"Rosenburg", "age": 78, "gender_id": 1,},
#         {"name":"Neil dGrasse Tyson", "age": 57, "gender_id": True,},
#         ]
#     )

# # gather records through accessing the table object (better performance)
# print(handler.database.tables["scientists"].records)

# # read the records from the database (performance heavy for large databases)
# print(handler.table_read_records(tablename="scientists"))

# # conditional read with where statement
# where = [["gender_id", [1]]]
# records = handler.table_read_records(tablename="scientists", where=where)
# print(f"read where")
# print_records(records)

# # update with where statement
# valuepairs = [["gender_id", 1]]
# where = [["name", ["Hawking"]]]
# records = handler.table_update_records(tablename="scientists", valuepairs=valuepairs, where=where)
# print(f"update true to false")
# print_records(records)

# # update with direct primary id
# valuepairs = [["name", "Neil de'Grasse Tyson"], ["age", 40]]
# primarykey = 5
# records = handler.table_update_records(tablename="scientists", valuepairs=valuepairs, where=primarykey)
# print(f"update record 'id = 5'")
# print_records(records)

# table_papers = handler.table_create(
#     tablename = "papers",
#     column_dict={
#         "name": "string",
#         "description": "str",
#     },
# )
# table_nobel = handler.table_create(
#     tablename = "ignorants",
#     column_dict={
#         "name": "string",
#         "description": "str",
#     },
# )

# # creating multiple records and return them (these are not yet saved in the database)
# records = handler.records_create(
#     tablename="nobelprizes",
#     recordsvalues=[
#         ["Peace", "Peace nobel prize"],
#         ["Economy", "Economy nobel prize"],
#         ["Physics", "Physics nobel prize"],
#         ["Sociology", "Sociology nobel prize"],
#         ]
#     )
# # adding these records to the database
# records = handler.table_add_records(tablename="nobelprizes", records=records)
# print(f"creating multiple records")
# print_records(records)

# # again
# records = handler.table_create_add_records(
#     tablename="papers",
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
#     tablename1="scientists",
#     tablename2="nobelprizes",
# )
# print(f"Database contains {handler.database.tables}")

# # adding a crossref table
# crossref_table = handler.crossref_create(
#     tablename1="scientists",
#     tablename2="papers",
# )
# print(f"Database contains {handler.database.tables}")

# # getting a crossref table
# crossref_table = handler.crossref_get(
#     tablename1="scientists",
#     tablename2="papers",
# )
# print(f"Crossref table is {crossref_table}")

# # get crossreferences
# crossreferences = handler.crossref_get_all(tablename="scientists")
# print(f"Table scientists has links to:")
# for crossreference in crossreferences:
#     print(f"- {crossreference.name}")

# # adding a crossref
# handler.crossref_add_record(
#     tablename1="scientists",
#     tablename2="nobelprizes",
#     where1=[
#         ["name", ["Hawking"]]
#         ],
#     where2=[
#         ["name", ["Economy"]]
#         ],
# )

# # adding a crossref
# handler.crossref_add_record(
#     tablename1="scientists",
#     tablename2="nobelprizes",
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
#     tablename1="scientists",
#     tablename2="papers",
#     where1=[1],
#     where2=[3, 4],
#     description="Hawking wrote papers about time and fear of time"
# )

# # reading crossreferences of all scientists and nobelprizes
# records = handler.crossref_read_records(
#     tablename1="scientists",
#     tablename2="nobelprizes",
#     )
# print_records(records)

# # reading crossreferences of a single scientist and papers
# records = handler.crossref_read_record(
#     tablename1="scientists",
#     tablename2="papers",
#     primarykey=1,
# )
# print(f"looking up Hawkings papers:")
# print_records(records)

# # deleting record
# where = [
#     ["name", ["Rosenburg"]]
# ]
# handler.table_delete_records(tablename="scientists", where=where)
# print(f"deleted record rosenburg")
# records = handler.table_read_records(tablename="scientists")
# print_records(records)

# # delete a table
# handler.table_delete(tablename="ignorants")

# # gathering all records of all tables
# recordset = []
# for tablename in handler.database.tables:
#     records = handler.table_read_records(tablename)
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
