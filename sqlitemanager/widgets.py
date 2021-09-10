
from PyQt5.QtCore import (
    Qt,
    )

from PyQt5.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QFrame,
    QGridLayout,
    QLabel,
    QLineEdit,
    QPushButton, 
    QSpinBox,
    QTextEdit,
    QVBoxLayout,
    )


class RecordLayout(QGridLayout):
    def __init__(self, mainwindow):
        super().__init__()

        """
        Builds a layout based upon a Table object
        keeps track of the widgets for easy manipulation
        """

        self.mainwindow = mainwindow
        # self.database = database ----------- self.mainwindow.handler.database
        # self.record = record --------------- self.mainwindow.record_selected
        self.widgets = []
        self.build_layout()

    def build_layout(self):

        self.build_detailbox()
        # self.build_childrenbox()
        # self.build_xrefbox()

    def build_detailbox(self):

        box = QGridLayout()
        table = self.mainwindow.table_selected
        # print(f"table = {table}")

        recordarray = self.mainwindow.record_selected.recordarray
        # print(f"recordarray = {recordarray}")

        for index, columntype in enumerate(table.column_types):

            columnname = table.column_names[index]

            # set title for widget
            widget_title = QLabel()
            widget_title.setText(columnname)

            # check if column is a foreign key, it needs at least 3 text fields between spaces (column name, fk denotion, fk column)
            fkfound = False
            # print(f"columntype.upper = {columntype.upper()}")
            if "REFERENCES" in columntype.upper():
                fkfound = True
                # print(f"index {index} and recordarray {recordarray}, found foreign key {columntype}")
                widget_value = QComboBox()

                # get the actual records of the foreign table
                foreign_records = self.mainwindow.handler.table_get_foreign_records(tablename=table.name, column=columnname)

                # Create first row for a no choice option
                widget_value.addItem(f"No {columnname}", 0)

                # get the id and name column of the foreign records
                for record_index, foreign_record in enumerate(foreign_records):
                    foreign_id = foreign_record.primarykey
                    foreign_name = foreign_record.recorddict["name"]

                    # add item with both a shown string (1) as well as a piece of data (2)
                    widget_value.addItem(foreign_name, foreign_id)

                    # below sets itemdata at a certain point (1), data itself (2) and what its used for (3)
                    # including the no choice, the actual combo index is plus 1
                    widget_tooltip = f"tooltip {foreign_name}"
                    widget_value.setItemData(record_index + 1, widget_tooltip, Qt.ToolTipRole)
                    # print(f"added {foreign_name} with {foreign_id}")

                    if recordarray[index] == foreign_id:
                        # setting the default value and the tooltip for the combobox itself
                        # print(f"record shows value {recordarray[index]}")
                        widget_value.setCurrentIndex(record_index + 1)
                        widget_value.setToolTip(widget_tooltip)

            # if fkfound is true then it was a foreign key and the widget is already made
            if fkfound == False:
                ctype = columntype.split(' ', 1)[0].upper()
                # print(f"ctype = {ctype}")
                # print(f"index {index} and recordarray {recordarray}")
                if ctype == "INTEGER":
                    widget_value = QSpinBox()
                    widget_value.setMinimum(-1)
                    widget_value.setValue(recordarray[index])

                elif ctype == "BOOL":
                    widget_value = QCheckBox()
                    widget_value.setChecked(recordarray[index])                            

                elif ctype == "VARCHAR(255)":
                    widget_value = QLineEdit()
                    widget_value.setText(recordarray[index])
                
                elif ctype == "TEXT":
                    widget_value = QTextEdit()
                    widget_value.adjustSize()
                    widget_value.insertPlainText(recordarray[index])
                    # widget_value.insertHtml(recordarray[index])

                # elif ctype == "DATE":
                #     widget_value = QDateEdit()
                #     date = QDate()
                #     sqldate = recordarray[index]
                #     datestring = datetime.date()
                #     date.fromString(recordarray[index], 'yyyy-MM-dd')
                #     widget_value.setDate(date)

                # elif ctype == "DATETIME" or ctype == "TIMESTAMP":
                #     widget_value = QDateTimeEdit()
                #     date = QDateTime()
                #     sqldate = recordarray[index]
                #     datestring = datetime.datetime()
                #     date.fromString(recordarray[index], 'yyyy-MM-dd')
                #     widget_value.setDate(date)

                else:
                    try:
                        # assumed text
                        widget_value = QLineEdit()
                        widget_value.setText(recordarray[index])
                    except:
                        widget_value = QLineEdit()
                        widget_value.setText("Error setting widget")

                # set focus if widget is "name"
                if table.column_names[index] == "name":
                    widget_value.setFocusPolicy(Qt.StrongFocus)

            # print(f"column placements are {table.column_placements}")
            # print(f"column placements[index] are {table.column_placements[index]}")
            row = table.column_placements[index][0]
            column = table.column_placements[index][1] + 1
            heigth = table.column_placements[index][2]
            width = table.column_placements[index][3]

            # add widget and title to the layout
            box.addWidget(widget_value, row, column, heigth, width)
            box.addWidget(widget_title, row, 0, heigth, 1)

            # add the value widget to the list of widgets for easy access of values
            self.widgets.append(widget_value)

        # finish  window
        frame = QFrame()
        frame.setLayout(box)
        self.addWidget(frame, 0,0,2,8)

    def build_childrenbox(self):
        """
        gather the one to many children of this record,
        so gather the tables that have a foreign key to this table
        and show the foreign records (children) belonging to this record

        i.e. if this record denotes the 'Roman Empire', collect all the countries belonging to it
        """
        box = QVBoxLayout()

        # for table in self.mainwindow.handler.database.tables:
        #     reference_text = f"{self.mainwindow.table_selected.name}(id)"
        #     for ctype in table.column_types:
        #         ctypesplit = ctype.split("_", 3)
        #         try:
        #             if ctypesplit[2] == reference_text:
        #                 widget = QLabel()
        #                 widget.setText(f"child found as {reference_text} in {table}")
        #                 box.addWidget(widget)

        #                 # add the value widget to the list of widgets for easy access of values
        #                 self.childwidgets.append(widget)

        #         except:
        #             pass           

        # finish window
        frame = QFrame()
        frame.setLayout(box)
        self.addWidget(frame, 0,9,1,1)

    def build_xrefbox(self):

        box = QVBoxLayout()


        
        # finish window
        frame = QFrame()
        frame.setLayout(box)
        self.addWidget(frame, 1,9,1,1)

    def processValues(self):
        """
        Returns a Record object, 
        """

        widgets = self.widgets
        table = self.mainwindow.table_selected
        record = self.mainwindow.record_selected

        recordarray = []
        for index, columntype in enumerate(table.column_types):
            
            # check if column is a foreign key, it needs at least 3 text fields between spaces (column name, fk denotion, fk column)
            fkfound = False

            split = columntype.split(' ', 3)
            # print(f"split {split}")

            cname = table.column_names[index]

            if len(split) == 3:
                creferences = split[1]
                cforeign = split[2]
                
                if creferences.upper() == "REFERENCES":
                    fkfound = True
                    currentindex = widgets[index].currentIndex()
                    currentid = widgets[index].itemData(currentindex)
                    # print(f"itemindex {currentindex} and itemdata {currentid}")
                    recordarray.append(currentid)

            if fkfound == False:
                ctype = columntype.split(' ', 1)[0].upper()
                # print(f"ctype = {ctype}")

                if ctype == "VARCHAR(255)":
                    recordarray.append(widgets[index].text())
                elif ctype == "TEXT":
                    recordarray.append(widgets[index].toPlainText())
                elif ctype == "INTEGER":
                    recordarray.append(widgets[index].value())
                elif ctype == "DATE":
                    recordarray.append(widgets[index].value())
                elif ctype == "BOOL":
                    recordarray.append(widgets[index].isChecked())

            # print(f"recordarray building {recordarray}")
        # print(f"recordarray processed {recordarray}")
        record = self.mainwindow.handler.record_create(
            tablename = table.name,
            recordarray = recordarray,
        )

        return record

class TableDialog(QDialog):
    def __init__(self, mainwindow):
        super().__init__()

        self.mainwindow = mainwindow
        self.layout = QFormLayout(self)
    
    def add_buttonbox(self):

        buttonBox = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, self)
        buttonBox.accepted.connect(self.accept)
        buttonBox.rejected.connect(self.reject)

        self.layout.addWidget(buttonBox)

class RecordTableDialog(TableDialog):
    def __init__(self, mainwindow):
        super().__init__(mainwindow)

        self.setWindowTitle("Create a new Record table")

        self.name = QLineEdit(self)
        self.name.setText("Tablename")
        self.name.setToolTip("Insert a table name, no spaces")

        self.recordname = QLineEdit(self)
        self.recordname.setText("Recordname")
        self.recordname.setToolTip("""
        Insert a record name. If left empty the record will have the table name minus the last letter (assuming table names are multiples of the records they represent
        """)

        self.column_names = QLineEdit(self)
        self.column_names.setText("")
        self.column_names.setToolTip("Input column names as a list of strings: 'col1, col2, col3, col4'.")

        self.column_types = QLineEdit(self)
        self.column_types.setText("")
        self.column_types.setToolTip("""
        Input column types as a list of strings:\n
        'INTEGER': contains a number\n
        'VARCHAR(255)': contains a single line of text\n
        'TEXT': contains large amount of text\n
        'BOOL': contains true / false\n
        \n
        'INTEGER REFERENCES <table>(id)': by filling in a table this column is linked to a parent table for their values\n
        i.e. "INTEGER REFERENCES characters(id)" links this column to the characters table\n
        and one can choose from a list of characters when creating a record.
        """)

        self.column_placements = QLineEdit(self)
        self.column_placements.setDisabled(True)
        self.column_placements.setToolTip("Not implemented: row, column, height, widt: [[1,0,1,1], [2,0,1,1], [3,0,1,1], [4,0,10,1]].")

        self.defaults = QLineEdit(self)
        self.defaults.setToolTip("""Insert default values: '0, "", "", True'""")

        self.linkbutton = QPushButton()
        self.linkbutton.setText("Link to other table")
        self.linkbutton.setToolTip("Click here to link this table to a versionized table or a cross reference table")
        self.linkbutton.clicked.connect(self.link_to)

        self.layout.addRow("Table name", self.name)
        self.layout.addRow("Record name", self.recordname)
        self.layout.addRow("Column names", self.column_names)
        self.layout.addRow("Column types", self.column_types)
        self.layout.addRow("Column Placements", self.column_placements)
        self.layout.addRow("Default values", self.defaults)
        self.layout.addRow("Link", self.linkbutton)

        self.add_buttonbox()
    
    def link_to(self):
        pass

    def createTable(self):

        column_names = [] if self.column_names.text() == "" else self.column_names.text().split(', ')
        column_types = [] if self.column_types.text() == "" else self.column_types.text().split(', ')
        column_placements = [] if self.column_placements.text() == "" else self.column_placements.text().split(', ')
        defaults = [] if self.defaults.text() == "" else self.defaults.text().split(', ')

        table = self.mainwindow.handler.table_create(
            tablename = self.name.text(),
            record_name = self.recordname.text(),
            column_names = column_names,
            column_types = column_types,
            column_placements = column_placements,
            defaults = defaults,
            )

        return table