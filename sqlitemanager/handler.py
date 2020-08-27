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

    def table_sync(self, table):
        
        sqlrecords = self.database.read_records(tablename=table.name, columns=table.column_names)
        table.records = self.transform_sql_to_record(table=table, sqlrecords=sqlrecords)

    def table_add_records(self, table, records):

        self.database.add_records(
            table,
            records,
        )

        self.table_sync(table)

    def table_update_records(self, table, valuepairs, where):
        """
        update specific records of the specified table

        if where is an integer, it will update the row with that primary key.
        otherwise a valuepair is expected in the form 
        [<columnname>, [<values]]
        """

        if isinstance(where, int):
            where = [["id", [where]]]

        self.database.update_records(
            tablename=table.name, 
            valuepairs = valuepairs,
            where=where,
        )

    def table_read_records(self, table, where=[]):

        sqlrecords = self.database.read_records(tablename=table.name, columns=table.column_names, where=where)
        records = self.transform_sql_to_record(table=table, sqlrecords=sqlrecords)
        # print(f"{self.name} records retrieved: {self.records}")

        return records

    def crossref_create(self, tablename1, tablename2):

        crossref_table = handler.database.create_table(
            name = f"CROSSREF_{tablename1}_{tablename2}",
            column_names=[f"{tablename1}_id", f"{tablename2}_id", "description"],
            column_types=[f"INTEGER REFERENCES {tablename1}(id)", f"INTEGER REFERENCES {tablename2}(id)", "TEXT"],
        )

        return crossref_table

    def crossref_get_all(self, table):
        
        crossreferences = []

        for key in self.database.tables:
            if ("CROSSREF" in key) and (table.name in key):
                crossreferences += [key]

        return crossreferences        

    def crossref_get_one(self, tablename1, tablename2):

        for key in self.database.tables:
            if ("CROSSREF" in key) and (tablename1 in key) and (tablename2 in key):
                crossreference = key
                break

        return crossreference 

    def crossref_add_record(table1, table2, where1, where2):
        


    def read_records(self, tablename, where=[]):

        records = self.table_read_records(
            table=self.database.tables[tablename], 
            where=where,
            )

        return records

    def record_create(self, table, values):
        """
        creates a draft record that can still be 
        manipulated before making it definitive

        can be added to the table through
        table_add_records method
        """
        lastrow = self.database.get_max_row(table.name)

        recordarray = [lastrow + 1] + values

        record = Record(
            table.column_names,
            recordarray,
        )
        print(f"created record with recordarray {record.recordarray}")
        return record

    def records_create(self, table, recordsvalues):
        
        records = []
        for values in recordsvalues:
            records += [self.record_create(table, values)]

        return records

    def get_table_max_row(self, table):

        self.database.get_max_row(table.name)

    def readForeignValues(self, table, column):
        """
        for a particular tableobject and column,
        checks if column is a foreign key
        if so finds the table it points to and gets the records.
        records will be returned as an array of record objects
        """

        columnindex = table.column_names.index(column)
        split = table.column_types[columnindex].split(' ', 3)
        if len(split) != 3:
            return
        
        if split[1].upper() != "REFERENCES":
            return
        
        foreign_table_name = split[2].split('(',1)[0]
        foreign_table = self.tables[foreign_table_name]
        print(f"foreign table {foreign_table}")

        recordobjects = foreign_table.readAllRecords()
        print(f"record objects of foreign table {recordobjects}")

        return recordobjects

    def find_cross_referenced_values(self, table, recordId=[]):
        """
        loops over all the tables in the database.
        If they start with CROSSREF, checks if the name contains this tablename.
        if they do, find also the table it points to
        get a complete inner join of table1 and table2 based upon the crossref table
        if recordId's are given, then filter the results for the listed id's (of table1)
        """

        for key, tableobject in self.tables:
            if ("CROSSREF" in key.upper()) and (table.name in key):
                keysplit = key.split('_', 3)
                table1 = keysplit[1]
                table2 = keysplit[2]
                print(f"linked table found with name {key} and object {tableobject}")

                if table1 == table.name:
                    # get records from table2 and add them to the records found
                    pass

                elif table2 == table.name:
                    # get records from table1 and add them to the records found
                    pass

    def create_crossref_table(self):
        """
        creates a cross reference table between two tables for a many to many or versionized relationship.
        """

        # creates table with name = CROSSREF-<table1>-<table2> , column names are <tablerecordname>_id

    def transform_sql_to_record(self, table, sqlrecords):

        # print(sqlrecords)

        records = []
        for record in sqlrecords:

            recordarray = []
            for value in record:
                recordarray += [value]

            recordobject = Record(table.column_names, recordarray)
            # print(f"recordarray: {recordobject.recordarray}")

            records += [recordobject]

        return records

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
    table_scientists = handler.database.create_table(
        name="scientists",
        column_names = ["ordering", "name", "age", "nobelprizewinner"],
        column_types = ["INTEGER", "Text", "Integer", "Bool"],
    )
    print(f"Database contains {handler.database.tables}")

    # adding multiple records
    records = []
    records += [handler.record_create(
        table_scientists,
        [1, "Hawking", 68, True],
        )]
    records += [handler.record_create(
        table_scientists,
        [2, "Edison's child said \"Apple!\"", 20, True],
        )]
    handler.table_add_records(table_scientists, records)
    print(f"creating multiple records")
    print_records(table_scientists.records)
    
    # adding single records
    records = []
    records += [handler.record_create(
        table_scientists,
        [3, "Einstein", 100, False],
        )]
    handler.table_add_records(table_scientists, records)
    print(f"creating single records")
    print_records(table_scientists.records)

    # adding multiple records
    records = []
    records += [handler.record_create(
        table_scientists,
        [4, "Rosenburg", 78, False],
        )]
    records += [handler.record_create(
        table_scientists,
        [5, "Neil dGrasse Tyson", 57, True],
        )]
    handler.table_add_records(table_scientists, records)
    print(f"creating multiple records")
    print_records(table_scientists.records)

    # conditional read with where statement
    where = [["nobelprizewinner", [True]]]
    records = handler.table_read_records(table=table_scientists, where=where)
    print(f"read where")
    print_records(records)

    # update with where statement
    valuepairs = [["nobelprizewinner", False]]
    where = [["nobelprizewinner", [True]], ["name", ["Hawking"]]]
    handler.table_update_records(table=table_scientists, valuepairs=valuepairs, where=where)
    records = handler.table_read_records(table=table_scientists)
    print(f"update true to false")
    print_records(records)

    valuepairs = [["name", "Neil de'Grasse Tyson"], ["age", 40]]
    rowid = 5
    handler.table_update_records(table=table_scientists, valuepairs=valuepairs, where=rowid)
    records = handler.table_read_records(table=table_scientists)
    print(f"update record 'id = 5'")
    print_records(records)

    # get records without table object, but with tablename
    records = handler.read_records("scientists")
    print(f"read all records")
    print_records(records)

    # adding a table
    table_nobel = handler.database.create_table(
        name = "nobelprizes",
        column_names=["ordering", "name", "description"],
        column_types=["INTEGER", "VARCHAR(255)", "TEXT"],
    )
    
    # adding multiple records in one go
    records = handler.records_create(
        table_nobel,
            [
            [1, "Peace", "Peace nobel prize"],
            [2, "Economy", "Economy nobel prize"],
            [3, "Physics", "Physics nobel prize"],
            [4, "Sociology", "Sociology nobel prize"],
            ]
        )
    handler.table_add_records(table_nobel, records)
    print(f"creating multiple records")
    print_records(table_nobel.records)

    # adding a crossref table
    crossref_table = handler.crossref_create(
        tablename1="scientists",
        tablename2="nobelprizes",
    )
    print(f"Database contains {handler.database.tables}")

    # get crossreferences
    crossreferences = handler.crossref_get(table_scientists)
    print(f"Table {table_scientists} has links to {crossreferences}")

    # adding a crossref
    handler.crossref_add_record(
        table1=table_scientists,
        table2=table_nobel,
        where1=[
            ["name", "Hawking"]
            ],
        where2=[
            ["name", "Economy"]
        ],
    )

    # adding a crossref

    # values = [1, 2, "hawking and edison"]
    # record = reltbl.createRecord(values)
    # print(f"create single record")
    # print_records([record])

    # records = reltbl.readRecords()
    # print(f"read all records")
    # print_records(records)

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