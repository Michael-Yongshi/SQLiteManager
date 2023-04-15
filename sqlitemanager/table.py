class Table(object):
    def __init__(self, db, name, metadata, records):
        super().__init__()

        self.db = db
        self.name = name
        self.metadata = metadata
        self.records = records

        if self.records != None:

            for record in self.records:
                record.table = self