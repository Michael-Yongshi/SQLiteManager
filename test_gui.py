import sys

from PyQt5.QtWidgets import QApplication

from sqlitemanager.mainwindow import SQLmainwindow

global app
app = QApplication(sys.argv)
global main
main = SQLmainwindow()
sys.exit(app.exec_())
