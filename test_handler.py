from sqlitemanager.handler import SQLiteHandler


def print_records(records):
    for record in records:
        print(f"primarykey: {record.primarykey}, recordpairs: {record.recordpairs}")


# connect to database
handler = SQLiteHandler()
handler.database_open(filename="science")
handler.database_delete()
handler.database_new(filename="science")

# add a table
handler.table_create(
    tablename="scientists",
        column_names = ["age", "gender_id"],
        column_types = ["Integer", "INTEGER REFERENCES genders(id)"],
    )
print(handler.database.tables)

# add some records directly in the database
handler.table_create_add_records(
    tablename="scientists",
    recordsvalues=[
        [1, "Hawking", 68, 2],
        [2, "Marie Curie", 20, 2],
        [3, "Einstein", 100, 1],
        [4, "Rosenburg", 78, 1],
        [5, "Neil dGrasse Tyson", 57, True],
        ]
    )

# gather records through accessing the table object (better performance)
print(handler.database.tables["scientists"].records)

# read the records from the database (performance heavy for large databases)
print(handler.table_read_records(tablename="scientists"))

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
primarykey = 5
records = handler.table_update_records(tablename="scientists", valuepairs=valuepairs, where=primarykey)
print(f"update record 'id = 5'")
print_records(records)

# adding more tables
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

# creating multiple records and return them (these are not yet saved in the database)
records = handler.records_create(
    tablename="nobelprizes",
    recordsvalues=[
        [1, "Peace", "Peace nobel prize"],
        [2, "Economy", "Economy nobel prize"],
        [3, "Physics", "Physics nobel prize"],
        [4, "Sociology", "Sociology nobel prize"],
        ]
    )
# adding these records to the database
records = handler.table_add_records(tablename="nobelprizes", records=records)
print(f"creating multiple records")
print_records(records)

# again
records = handler.table_create_add_records(
    tablename="papers",
    recordsvalues=[
        [1, "Palestine", "Extrapolation on the palestinian cause"],
        [2, "Wealth", "On the Wealth of Nations"],
        [3, "Time", "A brief history of time"],
        [4, "Fear", "Controlling your fear"],
        ]
    )
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

# getting a crossref table
crossref_table = handler.crossref_get(
    tablename1="scientists",
    tablename2="papers",
)
print(f"Crossref table is {crossref_table}")

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
    primarykey=1,
)
print(f"looking up Hawkings papers:")
print_records(records)

# deleting record
where = [
    ["name", ["Rosenburg"]]
]
handler.table_delete_records(tablename="scientists", where=where)
print(f"deleted record rosenburg")
records = handler.table_read_records(tablename="scientists")
print_records(records)

# delete a table
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

# clean up by closing database
handler.database_close()