
from PyQt5 import QtCore, QtGui, QtWidgets
import os
import sys
import copy

import findspark
findspark.init()
import pyspark
import os
from pyspark.sql import SparkSession, Row, HiveContext, Window
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import  pyspark.sql.functions  as F

import colnames
import coldict
import readfile
import utils
import operations_inputs
import calculs

from dialog_freq_conso import Ui_Dialog

MATRICE_CONSO = "Matrice de consommations hors pharmacie"
MATRICE_PHARMA = "Matrice de consommations (pharmacie uniquement)"

class dialog_freqconso(QtWidgets.QDialog):
    def __init__(self, base_complete, parent=None):
        super(dialog_freqconso, self).__init__(parent)

        self.setModal(True)
        self.uiq = Ui_Dialog()
        self.uiq.setupUi(self)
        self.df = base_complete

        self.uiq.combo_type_conso.addItems([" ", MATRICE_CONSO, MATRICE_PHARMA])
        self.uiq.combo_type_conso.currentTextChanged.connect(self.choix_type_conso)
        
        self.uiq.table_mods_pharma.setEnabled(False)
        self.uiq.button_enregistrer_conso.setEnabled(False)

        self.uiq.button_enregistrer_conso.clicked.connect(self.accept)
        self.uiq.button_annuler_conso.clicked.connect(self.reject)

        self.actes = []
        self.actes_pharma = []
        self.matrice_choisie = " "

    def choix_type_conso(self, text):

        if text == " ":
            self.uiq.table_mods_pharma.setEnabled(False)
            self.uiq.button_enregistrer_conso.setEnabled(False)

        else:
            self.matrice_choisie = text
            if self.actes == []:
                self.actes = self.df.select(colnames.CODE_ACTE).distinct().rdd.flatMap(lambda x: x).collect()
                self.actes_ph = [acte for acte in self.actes if acte.startswith(("ph", "Ph", "PH"))]

            self.uiq.table_mods_pharma.setEnabled(True)
            self.uiq.table_mods_pharma.setRowCount(0)

            for acte in self.actes_ph:
                rowPos = self.uiq.table_mods_pharma.rowCount()
                chk_box = QtWidgets.QCheckBox()

                self.uiq.table_mods_pharma.insertRow(rowPos)
                self.uiq.table_mods_pharma.setCellWidget(rowPos, 0, chk_box)
                self.uiq.table_mods_pharma.setItem(rowPos, 1, QtWidgets.QTableWidgetItem(acte))
            
            self.uiq.button_enregistrer_conso.setEnabled(True)
    
    def accept(self):

        nrow = self.uiq.table_mods_pharma.rowCount()

        for i in range(0, nrow):
            chk = self.uiq.table_mods_pharma.cellWidget(i, 0)
            mod = self.uiq.table_mods_pharma.item(i, 1).text() 

            if chk.isChecked():
                self.actes_pharma.append(mod)
        
        super().accept()

    def get_actes_pharma(self):
        return self.actes_pharma #list
    
    def get_choix_matrice(self):
        return self.matrice_choisie #string
            
        

