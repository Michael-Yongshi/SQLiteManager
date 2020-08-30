from database import (
    Database,
    Table,
    Record,
)

from helpers import (
    get_localpath,
    check_existance,
)

class SQLiteHandler(object):
    def __init__(self, filename="", path=""):
        
        self.database = None
        if filename != "":
            if path == "":
                path = get_localpath()
            
            self.database_init(filename = filename, path = path)

    def database_init(self, filename, path):
        # try:
        self.database = Database(
            filename=filename,
            path=path,
        )
        
        # except:
        #     print(f"Couldn't initialize database!")

    def database_new(self, filename, path=""):
        """
        initialises database only if it doesnt exist yet
        """
        if path == "":
            path = get_localpath()

        exists = check_existance(filename, path)

        if exists == True:
            print(f"Couldnt create database, database with filename {filename} at directory {path} already exists!")
        else:
            self.database_init(filename=filename, path=path)

    def database_saveas(self, filename, path=""):
        self.database.saveas_database(filename=filename, path=path)

    def database_open(self, filename, path=""):
        """
        initialises database only if it already exists
        """

        if path == "":
            path = get_localpath()

        exists = check_existance(filename, path)

        if exists == True:
            self.database_init(filename=filename, path=path)
        else:
            print(f"Couldnt open database, database with filename {filename} at directory {path} doesn't exist!")

    def database_close(self):

        if self.database != None:
            self.database.close_database()
            self.database = None
        else:
            print(f"Couldn't close database, not connected to one!")

    def database_delete(self):
        
        if self.database != None:
            self.database.delete_database()
            self.database = None
        else:
            print(f"Couldn't delete database, not connected to one!")

    def table_create(self, tablename, column_names=[], column_types=[]):
        """
        By default a table will have
        - ordering: to denote a desired order of the records, by default mirrors id column
        - name: a readable reference of this record, preferably will be unique, but is not mandatory

        so put only extra columns on top of these in the column names with a desired type. 
        If they are not given at all the table will have only the columns:
        - id
        - ordering
        - name
        """

        table = self.database.create_table(
                name=tablename,
                column_names = ["ordering", "name"] + column_names,
                column_types = ["INTEGER", "VARCHAR(255)"] + column_types,
            )
        
        return table

    def table_delete(self, tablename):

        table = self.database.tables[tablename]
        self.database.delete_table(table=table)

        print(f"table {tablename} deleted")

    def table_sync(self, tablename):
        
        table = self.database.tables[tablename]
        sqlrecords = self.database.read_records(tablename=tablename, columns=table.column_names)
        table.records = self.transform_sql_to_record(table=table, sqlrecords=sqlrecords)

    def table_add_records(self, tablename, records):

        table = self.database.tables[tablename]

        # get current last row
        lastrow = self.table_max_row(tablename)

        # add the records to the table in the database
        self.database.add_records(
            table,
            records,
        )

        # refresh tableobjects records
        self.table_sync(tablename)

        # get last row after update
        newlastrow = self.table_max_row(tablename)

        # get all the just created rows (in an array lastrow of 1 is the 2nd record, so using lastrow number gets the record after the actual last row)
        records = table.records[lastrow:newlastrow]

        # print(f"Added records:")
        # print_records(records)

        return records

    def table_update_records(self, tablename, valuepairs, where):
        """
        update specific records of the specified table

        if where is an integer or array of integers, it will update those specified rows based on primary key.
        otherwise a valuepair is expected in the form 
        [<columnname>, [<values]]
        """

        where = self.transform_where(where=where)

        records_to_be_updated = self.table_read_records(tablename, where=where)

        ids_to_be_updated = []
        for record in records_to_be_updated:
            ids_to_be_updated += [record.primarykey]
        ids_to_be_updated = [["id", ids_to_be_updated]]

        # the actual updating
        self.database.update_records(
            tablename=tablename, 
            valuepairs = valuepairs,
            where=where,
        )

        # refresh table records
        self.table_sync(tablename)

        print(f"ids to be updated {ids_to_be_updated}")
        # read updated records from primary key list
        records = self.table_read_records(tablename, where=ids_to_be_updated)

        return records

    def table_read_records(self, tablename, where=[]):

        table = self.database.tables[tablename]

        if where == []:
            records = table.records
        else:
            sqlrecords = self.database.read_records(tablename=tablename, columns=table.column_names, where=where)
            records = self.transform_sql_to_record(table=table, sqlrecords=sqlrecords)
            # print(f"{self.name} records retrieved: {self.records}")

        return records

    def table_get_foreign_table(self, tablename, column):
        """
        for a particular tableobject and column,
        checks if column is a foreign key
        if so finds the table it points to
        """

        table = self.database.tables[tablename]

        # check if the column points to a foreign key
        columnindex = table.column_names.index(column)
        split = table.column_types[columnindex].split(' ', 3)
        if len(split) != 3:
            return
        if split[1].upper() != "REFERENCES":
            return

        # get the reference to the foreign table
        foreign_table_name = split[2].split('(',1)[0]
        foreign_table = self.database.tables[foreign_table_name]
        # print(f"foreign table {foreign_table}")

        return foreign_table

    def table_get_foreign_records(self, tablename, column, where=[]):
        """
        for a particular tableobject and column,
        checks if column is a foreign key
        if so finds the table it points to and gets the records.
        if a where is given, only give the foreign value(s) that are linked to the found rows
        records will be returned as an array of record objects
        """

        foreign_table = self.table_get_foreign_table(tablename=tablename, column=column)
        
        # get the records from the foreign table
        records = self.table_read_records(foreign_table)
        # print(f"record objects of foreign table {records}")

        # get the foreign keys to filter on
        # records = self.table_read_records(tablename=tablename, where=where)
        # foreign_keys = []
        # for record in records:
        #     foreign_keys += [record.recorddict[column]]

        return records

    def table_delete_records(self, tablename, where=[]):

        table = self.database.tables[tablename]

        where = self.transform_where(where=where)

        records = self.table_read_records(tablename=tablename, where=where)
        self.database.delete_records(table=table, records=records)

        self.table_sync(tablename)

    def table_max_row(self, tablename):
        lastrow = self.database.get_max_row(tablename)
        return lastrow

    def crossref_create(self, tablename1, tablename2):
        """
        Creates a crossreference table for tables with given table names.
        Next to the columns that contain the foreign keys, a column for a description is made for additional info.
        """

        crossref_table = self.table_create(
            tablename = f"CROSSREF_{tablename1}_{tablename2}",
            column_names=[f"{tablename1}_id", f"{tablename2}_id", "description"],
            column_types=[f"INTEGER REFERENCES {tablename1}(id)", f"INTEGER REFERENCES {tablename2}(id)", "TEXT"],
        )

        return crossref_table

    def crossref_get_all(self, tablename):
        """
        loops over all the tables in the database.
        If they start with CROSSREF, checks if the name contains this tablename.
        if they do, find also the table it points to
        get an array of all the found tables and return it
        """

        tables = []

        for key in self.database.tables:
            if ("CROSSREF" in key) and (tablename in key):
                crossrefname = key
                key = key.replace("CROSSREF", '')
                key = key.replace(tablename, '')
                key = key.replace('_', '')

                table = self.database.tables[key]
                tables += [table]

        return tables       

    def crossref_get_one(self, tablename1, tablename2):
        """
        loops over all the tables in the database.
        If they start with CROSSREF and contain both the table names, find the table it points to
        return the found table
        """

        for tablename in self.database.tables:
            if ("CROSSREF" in tablename) and (tablename1 in tablename) and (tablename2 in tablename):
                table = self.database.tables[tablename]
                break

        return table

    def crossref_add_record(self, tablename1, tablename2, where1, where2, description=""):
        """
        adds a crossreference between tables 1 and 2 for the rows given by the where statements.
        Creates links for any combination of the found rows of table1 and table 2. So if the where statements point to multiple rows,
        like 1-3 in table 1 and 5-8 in table 2, it loops over all possible combinations:
        1 - 5
        1 - 6
        1 - 7
        1 - 8

        2 - 5
        2 - 6
        2 - 7
        2 - 8

        3 - 5
        3 - 6
        3 - 7
        3 - 8
        """

        table1 = self.database.tables[tablename1]
        table2 = self.database.tables[tablename2]

        # get the cross reference table
        crossref = self.crossref_get_one(
            tablename1 = tablename1,
            tablename2 = tablename2, 
            )

        # print(f"where1 is {where1}")
        # get record primary keys / row id's
        if isinstance(where1[0], int):
            rowids1 = where1
        else:
            rowids1 = []
            records = self.table_read_records(
                tablename = tablename1,
                where = where1,
            )
            for record in records:
                rowids1 += [record.primarykey]
        # print(f"rowids1 is {rowids1}")

        # print(f"where2 is {where2}")
        if isinstance(where2[0], int):
            rowids2 = where2
        else:
            rowids2 = []
            records = self.table_read_records(
                tablename = tablename2,
                where = where2,
            )
            for record in records:
                rowids2 += [record.primarykey]
        # print(f"rowids2 is {rowids2}")

        # determine if tables need to be switched places
        index1 = crossref.name.find(tablename1)
        index2 = crossref.name.find(tablename2)

        if (index1 == -1) or (index2 == -1):
            print(f"table1 {tablename1} or table2 {tablename2} not found")
            return

        # switch columns
        values1 = rowids1 if index1 < index2 else rowids2
        values2 = rowids2 if index1 < index2 else rowids1

        lastrow = self.table_max_row(tablename=crossref.name)

        records = []
        for value1 in values1:
            for value2 in values2:
                # default description
                if description == "":
                    description = f"Cross referenced {where1} to {where2}"

                record = self.record_create(
                    tablename=crossref.name,
                    values=[
                        lastrow + 1, f"{value1}-{value2}", value1, value2, description
                    ]
                )
                records += [record]

        self.table_add_records(crossref.name, records)

    def crossref_read_records(self, tablename1, tablename2):

        table = self.crossref_get_one(tablename1=tablename1, tablename2=tablename2)
        records = self.table_read_records(table.name)

        return records

    def crossref_read_record(self, tablename1, tablename2, rowid):
        """
        reads the crossreference table and retrieves only elements for the rowid given of table1
        """

        table = self.crossref_get_one(tablename1, tablename2)
        # print(table.name)
        records = self.table_read_records(table.name)
        
        # print(records)

        records_found = []
        for record in records:
            for valuepair in record.valuepairs:
                if valuepair[0] == tablename1 + "_id":
                    if valuepair[1] == rowid:
                        records_found += [record]
                        break

        return records

    def record_create(self, tablename, values=[]):
        """
        creates a draft record that can still be 
        manipulated before making it definitive
        if values are empty, takes default values of the columns

        can be added to the table through
        table_add_records method
        """

        table = self.database.tables[tablename]

        if values == []:
            recordarray = table.defaults
        else:
            recordarray = [-1] + values

        record = Record(
            table.column_names,
            recordarray,
        )
        # print(f"created record with recordarray {record.recordarray}")
        return record

    def records_create(self, tablename, recordsvalues):
        """
        creates multiple draft records that can still be 
        manipulated before making it definitive

        can be added to the table through
        table_add_records method
        """

        records = []
        for values in recordsvalues:
            records += [self.record_create(tablename, values)]

        return records

    def transform_sql_to_record(self, table, sqlrecords):
        """
        uses table object as input, as its used privately and not made to be called directly by using application
        """

        # print(sqlrecords)

        records = []
        for sqlrecord in sqlrecords:

            recordarray = []
            for value in sqlrecord:
                recordarray += [value]

            recordobject = Record(
                column_names=table.column_names, 
                recordarray=recordarray
                )
            # print(f"Transformed Record object with recordarray: {recordobject.recordarray}")

            records += [recordobject]

        return records

    def transform_where(self, where):

        # if where is an integer, create where to delete a that record with this primary key (id column)
        if isinstance(where, int):
            where = [["id", [where]]]
        
        # if where is an array of integers, create where to delete those records with these primary keys (id column)
        elif isinstance(where[0], int):
            where = [["id", where]]

        # otherwise, no transform is needed
        return where

def print_records(records):
    for record in records:
        print(f"primarykey: {record.primarykey}, recordpairs: {record.recordpairs}")

if __name__ == "__main__":

    # opening a database
    handler = SQLiteHandler()
    handler.database_open(filename="science")
    handler.database_delete()
    handler.database_new(filename="science")

    # adding a table
    table_scientists = handler.table_create(
        tablename="scientists",
        column_names = ["age", "gender_id"],
        column_types = ["Integer", "INTEGER REFERENCES genders(id)"],
    )
    print(f"Database contains {handler.database.tables}")

    # adding multiple records
    records = []
    records += [handler.record_create(
        tablename="scientists",
        values=[1, "Hawking", 68, 2],
        )]
    records += [handler.record_create(
        tablename="scientists",
        values=[2, "Marie Curie", 20, 2],
        )]
    records = handler.table_add_records(tablename="scientists", records=records)
    print(f"creating multiple records")
    print_records(records)
    
    # adding single records
    records = []
    records += [handler.record_create(
        tablename="scientists",
        values=[3, "Einstein", 100, 1],
        )]
    records = handler.table_add_records(tablename="scientists", records=records)
    print(f"creating single records")
    print_records(records)

    # adding multiple records
    records = []
    records += [handler.record_create(
        tablename="scientists",
        values=[4, "Rosenburg", 78, 1],
        )]
    records += [handler.record_create(
        tablename="scientists",
        values=[5, "Neil dGrasse Tyson", 57, True],
        )]
    records = handler.table_add_records(tablename="scientists", records=records)
    print(f"creating multiple records")
    print_records(records)

    # conditional read with where statement
    where = [["gender_id", [1]]]
    records = handler.table_read_records(tablename="scientists", where=where)
    print(f"read where")
    print_records(records)

    # update with where statement
    valuepairs = [["gender_id", 1]]
    where = [["name", ["Hawking"]]]
    records = handler.table_update_records(tablename="scientists", valuepairs=valuepairs, where=where)
    print(f"update true to false")
    print_records(records)

    # update with direct primary id
    valuepairs = [["name", "Neil de'Grasse Tyson"], ["age", 40]]
    rowid = 5
    records = handler.table_update_records(tablename="scientists", valuepairs=valuepairs, where=rowid)
    print(f"update record 'id = 5'")
    print_records(records)

    # get records without table object, but with tablename
    records = handler.table_read_records(tablename="scientists")
    print(f"read all records")
    print_records(records)

    # adding a table
    table_nobel = handler.table_create(
        tablename = "nobelprizes",
        column_names=["description"],
        column_types=["TEXT"],
    )
    table_papers = handler.table_create(
        tablename = "papers",
        column_names=["description"],
        column_types=["TEXT"],
    )
    table_nobel = handler.table_create(
        tablename = "ignorants",
        column_names=["description"],
        column_types=["TEXT"],
    )

    # adding multiple records in one go
    records = handler.records_create(
        tablename="nobelprizes",
        recordsvalues=[
            [1, "Peace", "Peace nobel prize"],
            [2, "Economy", "Economy nobel prize"],
            [3, "Physics", "Physics nobel prize"],
            [4, "Sociology", "Sociology nobel prize"],
            ]
        )
    records = handler.table_add_records(tablename="nobelprizes", records=records)
    print(f"creating multiple records")
    print_records(records)

    # adding multiple records in one go
    records = handler.records_create(
        tablename="papers",
        recordsvalues=[
            [1, "Palestine", "Extrapolation on the palestinian cause"],
            [2, "Wealth", "On the Wealth of Nations"],
            [3, "Time", "A brief history of time"],
            [4, "Fear", "Controlling your fear"],
            ]
        )
    records = handler.table_add_records(tablename="papers", records=records)
    print(f"creating multiple records")
    print_records(records)

    # adding a crossref table
    crossref_table = handler.crossref_create(
        tablename1="scientists",
        tablename2="nobelprizes",
    )
    print(f"Database contains {handler.database.tables}")

    # adding a crossref table
    crossref_table = handler.crossref_create(
        tablename1="scientists",
        tablename2="papers",
    )
    print(f"Database contains {handler.database.tables}")

    # get crossreferences
    crossreferences = handler.crossref_get_all(tablename="scientists")
    print(f"Table scientists has links to:")
    for crossreference in crossreferences:
        print(f"- {crossreference.name}")

    # adding a crossref
    handler.crossref_add_record(
        tablename1="scientists",
        tablename2="nobelprizes",
        where1=[
            ["name", ["Hawking"]]
            ],
        where2=[
            ["name", ["Economy"]]
            ],
    )

    # adding a crossref
    handler.crossref_add_record(
        tablename1="scientists",
        tablename2="nobelprizes",
        where1=[
            ["name", ["Einstein"]]
            ],
        where2=[
            ["name", ["Physics", "Peace"]]
            ],
        description="Einstein wins both peace and physics nobel prizes"
    )

    # adding crossreff based upon primary keys
    handler.crossref_add_record(
        tablename1="scientists",
        tablename2="papers",
        where1=[1],
        where2=[3, 4],
        description="Hawking wrote papers about time and fear of time"
    )

    # reading crossreferences of all scientists and nobelprizes
    records = handler.crossref_read_records(
        tablename1="scientists",
        tablename2="nobelprizes",
        )
    print_records(records)

    # reading crossreferences of a single scientist and papers
    records = handler.crossref_read_record(
        tablename1="scientists",
        tablename2="papers",
        rowid=1,
    )
    print(f"looking up Hawkings papers:")
    print_records(records)

    where = [
        ["name", ["Rosenburg"]]
    ]
    handler.table_delete_records(tablename="scientists", where=where)
    print(f"deleted record rosenburg")
    records = handler.table_read_records(tablename="scientists")
    print_records(records)

    handler.table_delete(tablename="ignorants")

    # gathering all records of all tables
    recordset = []
    for tablename in handler.database.tables:
        records = handler.table_read_records(tablename)
        recordset += [records]
    
    # printing all the records of all tables
    print(f"Database contains {handler.database.tables}")
    for records in recordset:
        print("")
        print_records(records)
  



    # records = reltbl.readForeignValues('charid1')

    # db.saveas_database(filename="backup")
    # db.close_database()

    # filename = "science"

    # db = Database(filename=filename)

    # teachertbl = db.create_table(
    #     name="teachers",
    #     column_names = ["ordering", "name", "age", "active"],
    #     column_types = ["INTEGER", "Text", "Integer", "Bool"],
    # )

    # db.delete_table(teachertbl)

    # table_scientists = db.get_table("scientists")

    # where = [["nobelprizewinner", [True]]]
    # records = table_scientists.readRecords(where=where)

    # table_scientists.deleteRecords(records)

    # for table in db.tables:
    #     print(f"printing for table: {table.name}")
    #     print_records(table.records)