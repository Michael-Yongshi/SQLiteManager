import logging

import os

from .objects import Table, Record

def create_table(db, config_dict, record_dict = {}):
    """
    Creates a table in the database based on a configuration dictionary (config_dict)
    
    the config dict is a nested dict where the table_name is the first key with as value a dict with the column names as keys
    these columns each have a dict containing the parameters of the columns

    column_type is the only mandatory parameter
    The function will convert automatically the column types to the right SQL query parameters for certain keywords (not caps sensitive)
    and can transform these common type terms, like int, integer, number, str, string, txt, text, date, time, dt, datetime

    the config dict can be filled with some optional parameters when you want them to be included

    primary_key, autonumber and not_null are simply set to true, like
    primary_key: True
    autonumber: True
    not_null: True

    Defaults need data given
    column_default: <value>

    Foreign keys can be given by setting the foreign_key with value a dict of the table and column it should point to.
    foreign_key:{<table>:<column>}

    Optionally you can add already records for tables by adding a list of record dicts to be added immediately after creating the table
    The record dicts are just a dict where column name is the key and the data is the value
    {os_table:[
            {"name":"Fedora","age":300},
            {"name":"Ubuntu","age":50}
    }
    
    """

    valuetext = []

    # first is the table_name
    for table_name in config_dict:
        table_dict = config_dict[table_name]

        columns = []

        for column_name in table_dict:
            column_dict = table_dict[column_name]

            column_text = f"{column_name}"

            # fetch column type
            column_type_transform = column_dict["column_type"].upper()

            if column_type_transform in("INT", "INTEGER", "NUMBER", "#"):
                column_type = "INTEGER"
            elif column_type_transform in("STR", "STRING", "TXT", "TEXT"):
                column_type = "TEXT"
            elif column_type_transform in("DATE", "TIME", "DT", "DATETIME"):
                column_type = "DATE"
            else:
                # otherwise keep exactly the same, sure the caller knows what he is doing
                column_type = column_dict["column_type"]

            column_text += f" {column_type}"

            if column_dict.get("primary_key", "Not Found") != "Not Found":
                column_text += " PRIMARY KEY"
            if column_dict.get("autonumber", "Not Found") != "Not Found":
                column_text += " AUTOINCREMENT"
            if column_dict.get("not_null", "Not Found") != "Not Found":
                column_text += " NOT NULL"

            column_default = column_dict.get("column_default", "Not Found")
            if column_default != "Not Found":
                if column_default == "now":
                    column_default = "current_timestamp"
                column_text += f" DEFAULT {column_default}"

            foreign_key_dict = column_dict.get("foreign_key", "Not Found")
            if foreign_key_dict != "Not Found":
                foreign_table = list(foreign_key_dict)[0]
                foreign_column = foreign_key_dict[foreign_table]
                column_text += f" REFERENCES {foreign_table}({foreign_column})"

            columns += [column_text]

        # transform columns to string format
        valuetext = ",\n".join(columns)

        # create variables text
        query = f"CREATE TABLE IF NOT EXISTS {table_name} (\n{valuetext}\n);"
        db.execute_query(query)

        table_record_dict = record_dict.get(table_name, "Not Found")
        if table_record_dict != "Not Found":
            create_records(db = db, table_name = table_name, records = table_record_dict)

def delete_table(db, table_name):
    """
    Deleting a table from the database, does not return anything
    """

    query = f"DROP TABLE {table_name}"
    db.execute_query(query)

    logging.warning(f"table {table_name} deleted")

def get_table_names(db):
    """
    Fetches and returns a list of the tables within the database.
    Its sorted alphabetically
    """

    query = "SELECT name FROM sqlite_master WHERE type='table';"
    cursor = db.execute_query(query=query)
    data = cursor.fetchall()

    table_names = []
    for datapoint in data:
        table_names += [datapoint[0]]

    table_names.sort()

    return table_names

def get_table_metadata(db, table_selection=[]):
    """
    Fetches information from the database about all or a selection of tables within the database
    It fetches columns with ordering and types and returns a dict of tables, columns and this metadata.

    {'scientist': {
        'id': 
            {'order': 0, 'type': 'INTEGER'}, 
        'name': 
            {'order': 1, 'type': 'TEXT'}, 
        'age': 
            {'order': 2, 'type': 'INTEGER'}, 
        'sex_id': 
            {'order': 3, 'type': 'INTEGER'}
        }
    }
    
    if only a single table is requested as string, the result will just be that table dicts contents
    {
    'id': 
        {'order': 0, 'type': 'INTEGER'}, 
    'name': 
        {'order': 1, 'type': 'TEXT'}, 
    'age': 
        {'order': 2, 'type': 'INTEGER'}, 
    'sex_id': 
        {'order': 3, 'type': 'INTEGER'}
    }
    """
    
    if table_selection == []:
        table_names = get_table_names(db)
    elif isinstance(table_selection,str):
        table_names = [table_selection]
    else:
        table_names = table_selection

    tables_dict = {}
    for table_name in table_names:

        # print(table)
        cursor = db.execute_query(f"PRAGMA table_info({table_name})")
        data = cursor.fetchall()

        table_dict = {}
        for datapoint in data:
            column_name = datapoint[1]
            column_order = datapoint[0]
            column_type = datapoint[2]

            column_dict = {
                    "order":column_order,
                    "type":column_type
                }
            table_dict.update({column_name:column_dict})

        tables_dict.update({table_name: table_dict})

        logging.info(f"metadata of table {table_name} is {tables_dict}")

    if isinstance(table_selection,str):
        for key in tables_dict:
            return tables_dict[key]
    else:
        return tables_dict

def get_table_column_names(db, table_selection=[]):
    """
    Fetches the column names of one or multiple tables.

    if table selection is a single table as string, it will return solely a list of column names within this table
    if the table selection is in list form, even if its a single table, it will return a dict of tables and their columns as value
    If its empty a dict containing all tables and their column names will be given
    """

    table_metadata = get_table_metadata(db, table_selection=table_selection)
    if isinstance(table_selection,str):
        column_names = []
        for column_name in table_metadata:
            column_names += [column_name]
        return column_names

    else:
        table_column_dict = {}
        for table_name in table_metadata:
            column_names = []
            for column_name in table_metadata[table_name]:
                column_names += [column_name]
            table_column_dict.update({table_name:column_names})

        return table_column_dict

def get_tables(db, table_selection=[]):
    """
    fetches table objects and returned in a dict
    {table_name: table_object, table_name2: tableo_bject2}

    if only a single table is requested, the result will just be that table object
    """

    if table_selection == []:
        table_names = get_table_names(db)
    elif isinstance(table_selection,str):
        table_names = [table_selection]
    else:
        table_names = table_selection

    # get metadata for table
    metadata_dict = get_table_metadata(db, table_selection=table_names)

    # for every table create table object and add to a dict
    tables_dict = {}
    for table_name in table_names:

        table_metadata = metadata_dict[table_name]
        table_records = get_records(db, table_name=table_name)

        table_object = Table(
            db = db,
            name = table_name, 
            metadata = table_metadata,
            records = table_records,
            )

        # create table object and add to tables dict
        tables_dict.update({table_name:table_object})

    if isinstance(table_selection,str):
        for key in tables_dict:
            return tables_dict[key]
    else:
        return tables_dict

def create_records(db, table_name, records):
    """
    Insert new records into a table, tablename should be a string
    records come in the form of a list of record dicts 
    [
    {column:value, column2:value},
    {column:value, column2:value}
    ]
    """

    # process records in list of dicts format
    for record in records:
        column_names = []
        values = []

        for column_name in record:
            column_names += [column_name]

            # string value needs to have added single quotes for the query to denote its a string
            value = f"'{record[column_name]}'" if isinstance(record[column_name], str) else record[column_name]

            values += [f"{value}"]

        columns_text = ", ".join(column_names)
        values_text = ", ".join(values)

        query = f"INSERT INTO {table_name}\n({columns_text})\nVALUES\n({values_text})\n;"

        db.execute_query(query)

def update_records(db, table_name, update_dict, where):
    """
    Update records into a table, tablename should be a string
    update dict comes into the form of to-be-updated columns

    to be updated columns are in the form of a dict
    {
        "column_name":value,
        "column_name2":value2,
    }

    where is in the form of a dict
    {
        "column_name":{
            "operator":">",
            "values":50
        }
    }

    """

    where = resolve_where(db=db, where=where, parameterised=False)

    update_list = []

    for column_name, value in update_dict.items():
        if isinstance(value, str):
            value = f"'{value}'"
        update_list += [f"{column_name} = {value}"]
    set_text = ",\n".join(update_list)

    where_text = where

    query = f"UPDATE {table_name} \nSET {set_text} {where_text}\n;"
    db.execute_query(query)


    # parameters = tuple()

    # # create set_placeholders
    # set_placeholders = ""
    # for valuepair in valuepairs:
    #     parameters += tuple([valuepair[1]])
    #     set_placeholders += valuepair[0] + " = ?, "
    # set_placeholders = set_placeholders[:-2]
    # # print(f"set_placeholders = {set_placeholders}")
    # # print(f"parameters = {parameters}")

    # # create where_placeholders
    # where_placeholders = ""
    # for statement in where:
    #     parameters += tuple(statement[1])
    #     where_placeholders += statement[0] + " = ? AND "
    # where_placeholders = where_placeholders[:-5]
    # # print(f"where_placeholders = {where_placeholders}")
    # # print(f"parameters = {parameters}")

    # query = f"UPDATE {table_name} SET\n{set_placeholders}\nWHERE\n{where_placeholders}\n;"
    # self.execute_parameterised_query(query, parameters)

def delete_records(db, table_name, where):

    where = resolve_where(db=db, where=where, parameterised=False)

    # parameters = []
    # for record in records:
    #     parameters += [record.primarykey]

    # placeholders = ", ".join("?" for _ in parameters)

    query = f"DELETE FROM {table_name} {where}"
    db.execute_query(query)

def get_record(db, table_name, columns=[], where={}):

    records = get_records(db, table_name, columns, where)
    if len(records) == 1:
        return records[0]
    else:
        logging.warning("No or multiple records found, please use 'get_records' when fetching multiple records or provide a unique where clause")

def get_latest_record(db, table_name, column_name):

    query = f"SELECT MAX({column_name}) FROM {table_name}"
    cursor = db.execute_query(query)
    result = cursor.fetchone()

    # if no record is found return 0
    if result == []:
        return None

    # else return the found value
    else:
        where = {column_name:{
        "operator":"=",
        "values":result[0]}}
        return get_record(db=db, table_name=table_name, where=where)

def get_records(db, table_name, columns=[], where={}):
    """
    fetches a selection of records of a table with optionally a selection of the columns and a where clause

    column selection is just an array of the column names or a string for just a single column

    where can be collected as {
        column name: {"operator":"==", values:""}, 
        column name2: {"operator":"in",values:[]}, 
        column_name3: {"operator":">", values:#},
    }

    where is in the form of a dict
    {
        "column_name":{
            "operator":">",
            "values":50
        }
    }
    """

    query = f"SELECT "

    if columns == []:
        column_line = "*"
        column_names = get_table_column_names(db=db, table_selection=table_name)
    elif isinstance(columns, str):
        column_line = columns
        column_names = [columns]
    else:
        column_line = ", ".join(columns)
        column_names = columns

    query += column_line

    whereline = resolve_where(db, where, parameterised=False)

    query = f"SELECT {column_line} from {table_name} {whereline}"
    cursor = db.execute_query(query)
    sqlrecords = (cursor.fetchall())

    records = []
    for sqlrecord in sqlrecords:
        records += [Record.create_from_sqlrecord(table_name=table_name, column_names=column_names, values=sqlrecord)]

    if records == []:
        logging.warning(f"No records found with Where: {whereline}")

    return records

def resolve_where(db, where, parameterised = False):
    
    if parameterised:
        # # TODO, parameters have to be added seperately for a parameterised query
        # parameters = tuple()
        # whereline = ""
        # if where != {}:
        #     whereline += "WHERE "
        #     for statement in where:
        #         parameters += tuple(statement[1])

        #         whereline += f"{statement[0]}"
        #         whereline += " IN ("
        #         whereline += ", ".join("?" for _ in statement[1])
        #         whereline += ") AND "
        #     whereline = whereline[:-5]
            # print(f"whereline {whereline}")
        # print(f"parameters = {parameters}")

        # query = f"SELECT {column_line} from {table_name} {whereline}"
        # cursor = db.execute_parameterised_query(query, parameters)
        pass

    else:
    
        whereline = ""
        if where != {}:
            whereline += "WHERE "

            wherelist = []
            for column_name in where:
                where_dict = where[column_name]
                operator = where_dict["operator"]
                
                values = where_dict["values"]
                if isinstance(values, list):
                    if isinstance(values[0],str):
                        values = "', '".join(values)
                        values = f"('{values}')"
                    else:
                        # mapping to convert int values in list to str values
                        values = map(str, values)
                        values = ", ".join(values)
                        values = f"({values})"
                elif isinstance(values,str):
                    values = f"'{values}'"

                wherelist += [f"{column_name} {operator} {values}"]
            
            whereline += " AND ".join(wherelist)

        return whereline

def create_xref_table(db, xref_dict, records=[]):
    """
    Creates a crossreference table for tables with given table names.
    Next to the columns that contain the foreign keys, a column for a description is made for additional info.
    """

    # get desired table names
    table_names = list(xref_dict)
    table_name_1 = table_names[0]
    table_name_2 = table_names[1]
    xref_table_name = f"CROSSREF_{table_name_1}_{table_name_2}"

    # get desired column names that are referenced
    column_name_1 = xref_dict[table_name_1]
    column_name_2 = xref_dict[table_name_2]
    xref_column_name_1 = f"{table_name_1}_{column_name_1}"
    xref_column_name_2 = f"{table_name_2}_{column_name_2}"

    # set up create table config dict.
    config_dict = {xref_table_name: {
            xref_column_name_1: {"column_type": "INTEGER", "foreign_key": {table_name_1:column_name_1}},
            xref_column_name_2: {"column_type": "INTEGER", "foreign_key": {table_name_2:column_name_2}},
    }}

    create_table(db=db, config_dict=config_dict)

    # if records != []:
    #     create_records(db=db, table_name=xref_table_name, records=records)

def create_xref_records(db, xref_record_dict):
    """
    will create a record in the crossreference table that satisfies the 'where' clause. 
    if the where clause contains the '=' or 'in' operator and the columns of the actual crossreference table
    the crossreference will be tried immediately without first looking up the rows 
    (should not be necessary so saves on performance)

    if this is not satisfied, it will first look up the necessary records satisfying the condition
    only then try to create a crossreference using those records used crossreference columns

    The following dict format is accepted to determine the crossreference
    
    xref_record_dict = {
        "table_name1: {
            "id":{
            "operator":"=",
            "values":[#]
            }
        },
        "table_name2: {
            "name":{
            "operator":"=",
            "values":["some value2"]
            }
        }
    }
    """

    # get desired table names
    table_names = list(xref_record_dict)
    table_name_a = table_names[0]
    table_name_b = table_names[1]

    # check correct xref table name and set the xref_dict parts to the correct table 1 and table 2 so you always end up with the same order
    xref_table_name, xref_column_name_1, xref_column_name_2, table_name_1, table_name_2, column_name_1, column_name_2, inverse = get_xref_table(db=db, table_name_a=table_name_a, table_name_b=table_name_b)

    table_1_where_dict = xref_record_dict[table_name_1]
    table_2_where_dict = xref_record_dict[table_name_2]

    # now we get the actually records that need to be linked for each side
    table_1_records = get_records(db=db, table_name=table_name_1, columns=column_name_1, where=table_1_where_dict)
    table_2_records = get_records(db=db, table_name=table_name_2, columns=column_name_2, where=table_2_where_dict)

    record_list = []
    # fill the records to be created dict with all matches from table1 with table 2
    for record_1 in table_1_records:
        for record_2 in table_2_records:
            record_dict = {xref_column_name_1:record_1.dict[column_name_1], xref_column_name_2:record_2.dict[column_name_2]}
            record_list += [record_dict]

    create_records(db=db, table_name=xref_table_name, records=record_list)

def get_xref_table(db, table_name_a, table_name_b):
    """
    finds an existing xref table between tables A and B

    returns
    :xref table name
    :xref columns 1 and 2
    :table names 1 and 2
    :associated columns for tables 1 and 2
    :inverse that denotes if table name A != table name 1
    """

    table_names_in_db = get_table_names(db=db)
    xref_table_name_asgiven = f"CROSSREF_{table_name_a}_{table_name_b}"
    xref_table_name_inverse = f"CROSSREF_{table_name_b}_{table_name_a}"

    if xref_table_name_asgiven in table_names_in_db:
        xref_table_name = xref_table_name_asgiven
        table_name_1 = table_name_a
        table_name_2 = table_name_b
        inverse = False

    elif xref_table_name_inverse in table_names_in_db:
        xref_table_name = xref_table_name_inverse
        table_name_1 = table_name_b
        table_name_2 = table_name_a
        inverse = True

    else:
        logging.critical(f"xref table with table names {table_name_1} and {table_name_2} not found")

    # get column names of the xref table
    column_names = get_table_column_names(db=db, table_selection=xref_table_name)
    xref_column_name_1 = column_names[0]
    xref_column_name_2 = column_names[1]

    # the column names of the crossreference is in the form "tablename_columnname", so below results in the actual column names they reference to
    column_name_1 = xref_column_name_1.split("_")[1]
    column_name_2 = xref_column_name_2.split("_")[1]

    return xref_table_name, xref_column_name_1, xref_column_name_2, table_name_1, table_name_2, column_name_1, column_name_2, inverse

def get_xref_records(db, source_table, target_table, source_where):
    """
    used to get records from a second table that are related to the selection of records of the first table
    
    source_table where dict
    target_table name
    
    """

    # get cross reference table between source and target
    xref_table_name, xref_column_name_1, xref_column_name_2, table_name_1, table_name_2, column_name_1, column_name_2, inverse = get_xref_table(db=db, table_name_a=source_table, table_name_b=target_table)

    source_column_name = column_name_1 if inverse == False else column_name_2
    target_column_name = column_name_2 if inverse == False else column_name_1
    source_xref_column_name = xref_column_name_1 if inverse == False else xref_column_name_2
    target_xref_column_name = xref_column_name_2 if inverse == False else xref_column_name_1

    # get relevant records from source table
    source_records = get_records(db=db, table_name=source_table, columns=source_column_name, where=source_where)

    xref_where_values = []
    for record in source_records:
        xref_where_values += [record.dict[source_column_name]]

    xref_where = {source_xref_column_name: {"operator":"in", "values": xref_where_values}}
    xref_records = get_records(db=db, table_name=xref_table_name, where=xref_where)
    # for record in xref_records:
    #     record.print()

    # get records from target table
    target_where_values = []
    for record in xref_records:
        target_where_values += [record.dict[target_xref_column_name]]
    target_where = {target_column_name: {"operator":"in", "values": target_where_values}}
    target_records = get_records(db=db, table_name=target_table, where=target_where)

    for record in target_records:
        record.print()

# def table_get_foreign_table(self, table_name, column):
#     """
#     for a particular tableobject and column,
#     checks if column is a foreign key
#     if so finds the table it points to
#     """

#     table = self.database.tables[table_name]

#     # check if the column points to a foreign key
#     columnindex = table.column_names.index(column)
#     split = table.column_types[columnindex].split(" ", 3)
#     if len(split) != 3:
#         return
#     if split[1].upper() != "REFERENCES":
#         return

#     # get the reference to the foreign table
#     foreign_table_name = split[2].split("(",1)[0]
#     foreign_table = self.database.tables[foreign_table_name]
#     # print(f"foreign table {foreign_table}")

#     return foreign_table

# def table_get_foreign_records(self, table_name, column, where=[]):
#     """
#     for a particular tableobject and column,
#     checks if column is a foreign key
#     if so finds the table it points to and gets the records.
#     if a where is given, only give the foreign value(s) that are linked to the found rows
#     records will be returned as an array of record objects
#     """

#     foreign_table = self.table_get_foreign_table(table_name=table_name, column=column)
    
#     # get the records from the foreign table
#     records = self.table_read_records(foreign_table.name)
#     # print(f"record objects of foreign table {records}")

#     # get the foreign keys to filter on
#     # records = self.table_read_records(table_name=table_name, where=where)
#     # foreign_keys = []
#     # for record in records:
#     #     foreign_keys += [record.recorddict[column]]

#     return records


# def crossref_get(self, table_name1, table_name2):
#     """
#     Gets a crossreference table for tables with given table names if it exists.
#     will check first the following combination
#     "CROSSREF_table_name1_table_name2"
#     and if table != found will check
#     "CROSSREF_table_name2_table_name1"

#     Returns the table if it is found
#     """

#     crossref_nominal = "CROSSREF_" + table_name1 + "_" + table_name2
#     crossref_inverse = "CROSSREF_" + table_name2 + "_" + table_name1
#     # print(crossref_nominal)
#     # print(crossref_inverse)

#     try:
#         crossref = self.database.get_table(crossref_nominal)
#         # print(f"crossref nominal found with {crossref}")

#     except:
#         crossref = self.database.get_table(crossref_inverse)
#         # print(f"crossref inverse found with {crossref}")
    
#     return crossref

# def crossref_get_all(self, table_name):
#     """
#     loops over all the tables in the database.
#     If they start with CROSSREF, checks if the name contains this table_name.
#     if they do, find also the table it points to
#     get an array of all the found tables and return it
#     """

#     tables = []

#     for key in self.database.tables:
#         if ("CROSSREF" in key) and (table_name in key):
#             key = key.replace("CROSSREF", "")
#             key = key.replace(table_name, "")
#             key = key.replace("_", "")

#             table = self.database.tables[key]
#             tables += [table]

#     return tables       

# #Depreciated
# def crossref_get_one(self, table_name1, table_name2):
#     """
#     loops over all the tables in the database.
#     If they start with CROSSREF and contain both the table names, find the table it points to
#     return the found table
#     """

#     for table_name in self.database.tables:
#         if ("CROSSREF" in table_name) and (table_name1 in table_name) and (table_name2 in table_name):
#             table = self.database.tables[table_name]
#             break

#     return table

# def crossref_add_record(self, table_name1, table_name2, where1, where2, description=""):
#     """
#     adds a crossreference between tables 1 and 2 for the rows given by the where statements.
#     Creates links for any combination of the found rows of table1 and table 2. So if the where statements point to multiple rows,
#     like 1-3 in table 1 and 5-8 in table 2, it loops over all possible combinations:
#     1 - 5
#     1 - 6
#     1 - 7
#     1 - 8

#     2 - 5
#     2 - 6
#     2 - 7
#     2 - 8

#     3 - 5
#     3 - 6
#     3 - 7
#     3 - 8
#     """

#     # get the cross reference table
#     crossref = self.crossref_get(
#         table_name1 = table_name1,
#         table_name2 = table_name2, 
#         )

#     # print(f"where1 is {where1}")
#     # get record primary keys / row id"s
#     if isinstance(where1[0], int):
#         rowids1 = where1
#     else:
#         rowids1 = []
#         records = self.table_read_records(
#             table_name = table_name1,
#             where = where1,
#         )
#         for record in records:
#             rowids1 += [record.primarykey]
#     # print(f"rowids1 is {rowids1}")

#     # print(f"where2 is {where2}")
#     if isinstance(where2[0], int):
#         rowids2 = where2
#     else:
#         rowids2 = []
#         records = self.table_read_records(
#             table_name = table_name2,
#             where = where2,
#         )
#         for record in records:
#             rowids2 += [record.primarykey]
#     # print(f"rowids2 is {rowids2}")

#     # determine if tables need to be switched places
#     index1 = crossref.name.find(table_name1)
#     index2 = crossref.name.find(table_name2)

#     if (index1 == -1) or (index2 == -1):
#         print(f"table1 {table_name1} or table2 {table_name2} not found")
#         return

#     # switch columns
#     values1 = rowids1 if index1 < index2 else rowids2
#     values2 = rowids2 if index1 < index2 else rowids1

#     lastrow = self.table_max_row(table_name=crossref.name)

#     records = []
#     for value1 in values1:
#         for value2 in values2:
#             # default description
#             if description == "":
#                 description = f"Cross referenced {where1} to {where2}"

#             record = self.record_create(
#                 table_name=crossref.name,
#                 values=[
#                     lastrow + 1, f"{value1}-{value2}", value1, value2, description
#                 ]
#             )
#             records += [record]

#     self.table_add_records(crossref.name, records)

# def crossref_read_records(self, table_name1, table_name2):

#     table = self.crossref_get_one(table_name1=table_name1, table_name2=table_name2)
#     records = self.table_read_records(table.name)

#     return records

# def crossref_read_record(self, table_name1, table_name2, primarykey):
#     """
#     reads the crossreference table and retrieves only elements for the rowid given of table1
#     """

#     table = self.crossref_get_one(table_name1, table_name2)
#     # print(table.name)
#     records = self.table_read_records(table.name)
    
#     # print(records)

#     records_found = []
#     for record in records:
#         for valuepair in record.valuepairs:
#             if valuepair[0] == table_name1 + "_id":
#                 if valuepair[1] == primarykey:
#                     records_found += [record]
#                     break

#     return records_found

# def record_create(self, table_name, values=[], recordarray=[], recorddict={}):
#     """
#     creates a draft record that can still be 
#     manipulated before making it definitive
#     if values are empty, takes default values of the columns

#     can be added to the table through
#     table_add_records method
#     """

#     table = self.database.tables[table_name]

#     # if recordarray is given, take it as is
#     if recordarray != []:
#         pass

#     # if instead only values is given, add a new id in front and take that as recordarray
#     # (this will be a new record in the database and has therefore no id yet)
#     elif values != []:
#         recordarray = [-1] + values

#     elif recorddict != {}:
#         recordarray = [-1]
#         for column_name in table.column_names:
#             try:
#                 values += [recorddict[column_name]]
#             except:
#                 values += []

#     # if nothing is given, fill the values with default values
#     else:
#         recordarray = table.defaults

#     record = Record(
#         table.column_names,
#         recordarray,
#     )
#     # print(f"created record with recordarray {record.recordarray}")
#     return record

# def records_create(self, table_name, recordsvalues =[], recorddicts = []):
#     """
#     creates multiple draft records that can still be 
#     manipulated before making it definitive

#     can be added to the table through
#     table_add_records method
#     """

#     records = []

#     if recordsvalues != []:

#         for values in recordsvalues:
#             records += [self.record_create(table_name, values=values)]

#     elif recorddicts != []:
#         for recorddict in recorddicts:
#             records += [self.record_create(table_name, recorddict=recorddict)]

#     return records

# def transform_where(self, where):
    
#     # if where is an integer, create where to delete a that record with this primary key (id column)
#     if isinstance(where, int):
#         where = [["id", [where]]]
    
#     # if where is an array of integers, create where to delete those records with these primary keys (id column)
#     elif isinstance(where[0], int):
#         where = [["id", where]]

#     # otherwise, no transform is needed
#     return where

# def check_existance(self):

#     if os.path.exists(self.get_complete_path()):
#         return True
#     else:
#         return False


# def get_table(self, table_name):
    
#     retrieved_table = self.tables[table_name]
#     print(retrieved_table)

#     return retrieved_table





# def add_records(self, table, records):

#     values = []
#     for record in records:
#         values += [record.values]

#     self.create_records(
#         table_name = table.name,
#         column_names = table.column_names[1:],
#         valuepairs = values,
#     )



