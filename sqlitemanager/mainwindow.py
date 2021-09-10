
from PyQt5.QtWidgets import (
    QAction,
    QFileDialog,
    QGridLayout,
    QHBoxLayout,
    QInputDialog,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton, 
    QScrollArea,
    QSizePolicy,
    QVBoxLayout,
    )

from PyQt5.QtGui import (
    QIcon,
    )
    
from darktheme.widget_template import *
from darktheme.decorators import Decorators

from .widgets import (
    RecordLayout,
    RecordTableDialog,
    )
from .handler import SQLiteHandler

from .helpers import (
    check_existance,
    get_localpath,
    split_complete_path,
    )

class SQLmainwindow(QMainWindow):
    """The main window that everything runs in"""
    def __init__(self):
        super().__init__()

        # Some window settings
        self.setWindowTitle('SQL Manager Gui')
        self.setWindowIcon(QIcon('globe-23544_640.ico'))

        # file settings
        self.filename = None
        self.path = None
        self.handler = SQLiteHandler()

        # selection settings
        self.show_hidden_tables = False
        self.table_selected = None # Table object that is selected
        self.table_records = [] # list of records from the selected table
        self.record_selected = None # Record object that is selected
        self.record_array = [] # the array of values from the selected record

        # set menu bar
        self.initMenu()

        # set UI
        self.initUI()

    def initMenu(self):

        bar = self.menuBar()

        filemenu = bar.addMenu("File")
        buttons = [
            {
                "name": "New",
                "shortcut": "Ctrl+N",
                "tooltip": "Create new world",
                "connect": self.new_database
            },
            {
                "name": "Open",
                "shortcut": "Ctrl+O",
                "tooltip": "Open existing world",
                "connect": self.open_database
            },
            # {
            #     "name": "Save",
            #     "shortcut": "Ctrl+S",
            #     "tooltip": "Save current world",
            #     "connect": self.save_database
            # },
            {
                "name": "SaveAs",
                "shortcut": "",
                "tooltip": "Save current world as ...",
                "connect": self.saveas_database
            },
            {
                "name": "Close",
                "shortcut": "",
                "tooltip": "Close current world",
                "connect": self.close_database
            },
            {
                "name": "Quit",
                "shortcut": "Ctrl+Q",
                "tooltip": "Quit application",
                "connect": self.close
            },
        ]
        for button in buttons:
            btn = QAction(button["name"], self)
            btn.setShortcut(button["shortcut"])
            btn.setStatusTip(button["tooltip"])
            btn.triggered.connect(button["connect"])
            filemenu.addAction(btn)

        tablemenu = bar.addMenu("Table")
        buttons = [
            {
                "name": "New",
                "shortcut": "Ctrl+N",
                "tooltip": "Create new world",
                "connect": self.new_table

            },
            # {
            #     "name": "Open",
            #     "shortcut": "Ctrl+O",
            #     "tooltip": "Open existing world",
            #     "connect": self.open_database
            # },
            # {
            #     "name": "Save",
            #     "shortcut": "Ctrl+S",
            #     "tooltip": "Save current world",
            #     "connect": self.save_database
            # },
            # {
            #     "name": "SaveAs",
            #     "shortcut": "",
            #     "tooltip": "Save current world as ...",
            #     "connect": self.saveas_database
            # },
            {
                "name": "Toggle hidden tables",
                "shortcut": "",
                "tooltip": "Toggle show hidden tables on or off",
                "connect": self.toggle_hidden_tables
            },
            {
                "name": "Delete",
                "shortcut": "Ctrl+Q",
                "tooltip": "Quit application",
                "connect": self.delete_table
            },
        ]
        for button in buttons:
            btn = QAction(button["name"], self)
            btn.setShortcut(button["shortcut"])
            btn.setStatusTip(button["tooltip"])
            btn.triggered.connect(button["connect"])
            tablemenu.addAction(btn)

    def initUI(self):     
        
        # build overview
        nested_widget = self.set_nested_widget()

        self.setCentralWidget(nested_widget)
        self.showMaximized()

        # self.widgets[1].setFocus()

    def set_nested_widget(self):

        overviewbox = QGridLayout()

        if self.handler.database != None:
            # get records from database if there is a table selected
            self.get_records()
            
            # vertical layout for left and right part
            overviewbox.addWidget(self.set_navbox(), 0, 0, 1, 1)
            overviewbox.addWidget(self.set_pagebox(), 0, 1, 1, 3)
        # build a startup window if filename is empty
        else:
            overviewbox.addWidget(self.startup_window())

        overviewboxframe = QBorderlessFrame()
        overviewboxframe.setLayout(overviewbox)

        return overviewboxframe

    def startup_window(self):

        startbox = QVBoxLayout()

        newbtn = QPushButton()
        newbtn.setText("New")
        newbtn.clicked.connect(self.new_database)
        startbox.addWidget(newbtn)

        openbtn = QPushButton()
        openbtn.setText("Open")
        openbtn.clicked.connect(self.open_database)
        startbox.addWidget(openbtn)

        startframe = QBorderlessFrame()
        startframe.setLayout(startbox)
        startframe.setFixedSize(200, 100)
        
        return startframe

    def set_navbox(self):

        # vertical layout for left and right part
        navbox = QVBoxLayout()

        # filenamelabel = QLabel()
        # filenamelabel.setText(f"World openend: <b>{self.handler.database.filename}</b>")
        # navbox.addWidget(filenamelabel)

        scrolltables = QScrollArea()
        btnlist = QVBoxLayout()
        tables = self.handler.database.tables
        for tablename in tables:
            if (("CROSSREF" in tablename) or ("VERSION" in tablename) or ("FIXEDPARENT" in tablename)) and (self.show_hidden_tables == False):
                pass
            else:
                btn = QPushButton()
                btn.setText(tablename)
                if self.table_selected != None:
                    if self.table_selected.name == tablename:
                        btn.setEnabled(False)
                if btn.isEnabled() == True:
                    btn.clicked.connect(self.closure_set_table(table=tables[tablename]))
                btnlist.addWidget(btn)
        frametables = QRaisedFrame()
        frametables.setLayout(btnlist)
        scrolltables.setWidget(frametables)
        scrolltables.setWidgetResizable(True)
        navbox.addWidget(scrolltables)

        scrollrecords = QScrollArea()
        listwidget = QListWidget()
        # print(f"table records {self.table_records}")
        listwidget.addItem(QListWidgetItem("***New Record***"))
        if self.table_records != []:
            for record in self.table_records:
                # print(record.values)
                name = record.name # take name as first value after primary key
                listwidgetitem = QListWidgetItem(f"{name}")
                listwidgetitem.setData(1, record)
                listwidget.addItem(listwidgetitem)
            listwidget.itemClicked.connect(self.set_record_from_widget_item)
        scrollrecords.setWidget(listwidget)
        scrollrecords.setWidgetResizable(True)
        navbox.addWidget(scrollrecords)

        # btntblnew = QPushButton()
        # btntblnew.setText("New Table")
        # btntblnew.clicked.connect(self.new_table)
        # navbox.addWidget(btntblnew)

        navboxframe = QRaisedFrame()
        navboxframe.setLayout(navbox)

        # set the list box to fixed horizontal size to avoid filling up the page when latter is empty
        navboxframe.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)

        return navboxframe      

    def set_pagebox(self):

        pagebox = QVBoxLayout()

        # we will create a gridlayout of the selected record with all the widgets and a seperate array with the widgets in order to manipulate them and pull values
        if self.record_selected != None:
            self.record_layout = RecordLayout(self)
        else:
            self.record_layout = QGridLayout()
        record_frame = QRaisedFrame()
        record_frame.setLayout(self.record_layout)
        pagebox.addWidget(record_frame, 15)

        # adding a new, create or edit button
        buttonbox = QHBoxLayout()
        
        if self.table_selected != None:
            
            if self.record_selected == None:
                newbtn = QPushButton()
                newbtn.setText("New Record")
                newbtn.clicked.connect(self.create_record)
                buttonbox.addWidget(newbtn, 1)

                delbtn = QPushButton()
                delbtn.setText("Delete Record")
                delbtn.clicked.connect(self.delete_record)
                buttonbox.addWidget(delbtn, 1)

            elif self.record_selected != None:
                if self.record_selected.primarykey == -1:
                    createbtn = QPushButton()
                    createbtn.setText("Confirm Creation")
                    createbtn.setShortcut("Ctrl+S")
                    createbtn.clicked.connect(self.add_record)
                    buttonbox.addWidget(createbtn, 1)

                else:
                    updatebtn = QPushButton()
                    updatebtn.setText("Confirm Update")
                    updatebtn.setShortcut("Ctrl+S")
                    updatebtn.clicked.connect(self.update_record)
                    buttonbox.addWidget(updatebtn, 1)

                    delbtn = QPushButton()
                    delbtn.setText("Delete")
                    delbtn.clicked.connect(self.delete_record)
                    buttonbox.addWidget(delbtn, 1)
        
        buttonframe = QRaisedFrame()
        buttonframe.setLayout(buttonbox)
        pagebox.addWidget(buttonframe, 1)

        pageboxframe = QRaisedFrame()
        pageboxframe.setLayout(pagebox)

        # set the page box to expanding horizontal size to make sure it fills up the space, disregarding if it has content or not
        pageboxframe.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        
        return pageboxframe

    def new_database(self):
        """Create a new world"""

        if self.handler.database != None:
            confirm = QMessageBox.question(self, 'Are you sure?', f"There is currently a world loaded.\nDo you want to create a new world?", QMessageBox.Yes | QMessageBox.No)
            if confirm == QMessageBox.No:
                return
        
        path = get_localpath()

        filetuple = QFileDialog.getSaveFileName(self, 'Save location', path, "SQLite3 databases (*.sqlite)")
        path, filename = split_complete_path(filetuple[0])

        if filename != "":
            print(f"path {path} and filename {filename}")

            if check_existance(path=path, filename=filename) == False:
                self.close_database()
                self.handler.database_new(filename=filename, path=path)
                self.initUI()

            else:
                QMessageBox.warning(self, "Couldn't create world!", f"database with {filename} already exists!", QMessageBox.Ok)

    def open_database(self, filename="", path=""):

        # if filename is not given
        if filename == "" or filename == False:

            filetuple = QFileDialog.getOpenFileName(self, 'Open file', get_localpath(), "SQLite3 databases (*.sqlite)")
            path, filename = split_complete_path(filetuple[0])

        if filename != "" and filename != None:
            # print(filename)
            
            # clean data
            self.close_database()
            self.handler.database_open(filename=filename, path=path)
            self.initUI()

    def save_database(self):
        pass

    def saveas_database(self):

        filetuple = QFileDialog.getSaveFileName(self, 'Save location', self.handler.database.path, "SQLite3 databases (*.sqlite)")
        path, filename = split_complete_path(filetuple[0])

        if filename != "":
            if check_existance(path=path, filename=filename) == False:

                # save the database under a different name and then select the new database
                self.handler.database_saveas(filename=filename, path=path)
                self.open_database(filename=filename, path=path)

                QMessageBox.information(self, "Saved", "Save successful!", QMessageBox.Ok)

            else:
                QMessageBox.warning(self, "Couldn't create world!", f"database with {filename} already exists!", QMessageBox.Ok)

    def close_database(self):
        
        # clean data
        self.handler.database_close()
        self.table_selected = None # Table object
        self.table_records = [] # list of records from table_selected
        self.record_selected = None
        self.record_array = []

    def get_records(self):

        self.table_records = []

        if self.table_selected != None:

            self.table_records = self.handler.table_read_records(self.table_selected.name)
            # print(self.table_records)
    
    def new_table(self):

        # what kind of table
        rtable = "Record table"
        ptable = "Parent table"
        fptable = "Fixed parent table"
        crtable = "Cross reference table"
        vtable = "Versionized table"
        tabletypes = [rtable, ptable, fptable, crtable, vtable]

        description = """
        What kind of table do you want to add?\n\n
        -- Record table: a table to input new records.\n
        -- Parent table: a table containing groups or collections.\n
        -- Fixed parent table: a fixed parent table, a table containing groups or collections that are hidden from view by default.\n
        -- Versionized table: a conditional parent table, if groups or collections are only valid under some condition, i.e. time.\n
        -- Cross reference table: a table linking normal tables together, not shown in table list.
        """

        tabletype, okPressed = QInputDialog.getItem(self, "Type of table", description, tabletypes)

        if tabletype and okPressed:
            if tabletype == rtable:
                print(f"Create {tabletype}")
                dialog = RecordTableDialog(self)

            if dialog.exec():
                self.table_selected = dialog.createTable()
                self.initUI()
                QMessageBox.information(self, "Success", f"Created net table with name {self.table_selected.name}", QMessageBox.Ok)

        else:
             print("Canceled creation of table")

    def closure_set_table(self, table):
        def set_table():

            self.set_table(table)

        return set_table

    def set_table(self, table):

        self.table_selected = table
        self.record_selected = None
        self.initUI()

    def set_record_from_widget_item(self, widgetitem):
        
        if widgetitem.text() == "*New Record":
            self.create_record()
        else:
            self.record_selected = widgetitem.data(1)
        self.initUI()

    def set_record(self, record):

        self.record_selected = record
        self.initUI()

    def create_record(self):

        self.record_selected = self.handler.record_create(tablename=self.table_selected.name)
        self.initUI()

    def add_record(self):

        # get a Record object for the new record
        newrecord = self.record_layout.processValues()
        # print(f"newrecord with values {newrecord.values}")

        # create the new record in database and retrieve the new record from database
        records = self.handler.table_add_records(tablename=self.table_selected.name, records=[newrecord])
        record = records[0]
        # print(f"record {newrecord.recordarray}")

        # set the selected record to the new record
        self.set_record(record)

        self.initUI()

    def update_record(self):

        # get a Record object for the new record
        updaterecord = self.record_layout.processValues()

        # update the record in database and retrieve the updated record from database
        records = self.handler.table_update_records(tablename=self.table_selected.name, valuepairs=updaterecord.valuepairs, where=updaterecord.primarykey)
        # print(records)
        record = records[0]
        
        self.set_record(record)
        self.initUI()

    def delete_record(self):

        self.handler.table_delete_records(tablename=self.table_selected.name, where=self.record_selected.primarykey)
        self.record_selected = None
        self.get_records()
        self.initUI()

    def delete_table(self):

        if self.table_selected != None:
            self.handler.database.delete_table(self.table_selected)
            self.close_database()
            self.initUI()
        else:
            messagebox = QMessageBox.warning(self, "Error", "No table selected. \nPlease select a table first.")

    def toggle_hidden_tables(self):
        self.show_hidden_tables = True if self.show_hidden_tables == False else False
        self.initUI()
