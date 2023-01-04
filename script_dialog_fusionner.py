#Script dialog fusionner

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

import readfile
import colnames
import coldict
import calculs

LABEL_BASE_ASSIETTE = "DEMOGRAPHIQUE"
LABEL_BASE_CONSO = "CONSOMMATIONS"
LABEL_BASE_ALD =  "ALD/ALC"
LABEL_BASE_MEDIC = "MEDICAMENTS"

from dialog_fusion import Ui_Dialog

class dialog_fusionner(QtWidgets.QDialog):
    def __init__(self, dict_df : dict, sources : list, parent = None):
        super(dialog_fusionner, self).__init__(parent)

        self.setModal(True)
        self.uif = Ui_Dialog()
        self.uif.setupUi(self)
        self.tuple_df = dict_df # {(base, année d'inventaire) : dataframe spark}

        self.dict_df = {str(k) : k for k in dict_df.keys()}

        bases = list(self.dict_df.keys())

        self.uif.combo_base_g.addItems([" "] + bases)
        self.uif.combo_base_d.addItems([" "] + bases)

        self.uif.combo_join_type.addItems([" "] + ["left", "right", "full"])

        self.uif.combo_base_d.currentTextChanged.connect(self.debloquer_cle_d)
        self.uif.combo_base_g.currentTextChanged.connect(self.debloquer_cle_g)

        self.uif.combo_cle_d.currentTextChanged.connect(self.cle_d)
        self.uif.combo_cle_g.currentTextChanged.connect(self.cle_g)

        self.uif.combo_join_type.currentTextChanged.connect(self.join_type)

        self.uif.button_fusionner.clicked.connect(self.accept)
        
        self.uif.combo_cle_d.setSizeAdjustPolicy(QtWidgets.QComboBox.AdjustToContents)
        self.uif.combo_cle_g.setSizeAdjustPolicy(QtWidgets.QComboBox.AdjustToContents)

        self.uif.combo_cle_d.setEnabled(False)
        self.uif.combo_cle_g.setEnabled(False)
        self.uif.combo_join_type.setEnabled(False)
        self.uif.button_fusionner.setEnabled(False)
        
    def debloquer_cle_d(self, text):
        if text == " ":
            self.uif.combo_cle_d.setEnabled(False)
            return
        
        self.uif.combo_cle_d.setEnabled(True)
        self.uif.combo_cle_d.clear()
        self.uif.combo_cle_d.addItems([" "] + self.tuple_df[self.dict_df[text]].columns)
    
    def debloquer_cle_g(self, text):
        if text == " ":
            self.uif.combo_cle_g.setEnabled(False)
            return

        self.uif.combo_cle_g.setEnabled(True)
        self.uif.combo_cle_g.clear()
        self.uif.combo_cle_g.addItems([" "] + self.tuple_df[self.dict_df[text]].columns)
    
    def cle_d(self, text):
        if text == " " or self.uif.combo_cle_g.currentText() == " ":
            self.uif.combo_join_type.setEnabled(False)
            return
        self.uif.combo_join_type.setEnabled(True)

    def cle_g(self, text):
        if text == " " or self.uif.combo_cle_d.currentText() == " ":
            self.uif.combo_join_type.setEnabled(False)
            return
        self.uif.combo_join_type.setEnabled(True)

    def join_type(self, text):

        if text == " ":
            return

        self.base_g = self.uif.combo_base_g.currentText()
        self.base_d = self.uif.combo_base_d.currentText()

        df_d = self.tuple_df[self.dict_df[self.base_d]]

        self.cle_g = self.uif.combo_cle_g.currentText()
        self.cle_d = self.uif.combo_cle_d.currentText()

        self.type_jointure = self.uif.combo_join_type.currentText()

        self.vars_d = df_d.columns
        self.vars_d.pop(self.vars_d.index(self.cle_d))

        for var in self.vars_d:
            rowPos = self.uif.table_variables_a_importer.rowCount()
            self.uif.table_variables_a_importer.insertRow(rowPos)

            chk_box = QtWidgets.QCheckBox()

            self.uif.table_variables_a_importer.setCellWidget(rowPos, 0, chk_box)
            self.uif.table_variables_a_importer.setItem(rowPos, 1, QtWidgets.QTableWidgetItem(var))
        
        self.uif.button_fusionner.setEnabled(True)
    
    def accept(self):
        nrow = self.uif.table_variables_a_importer.rowCount()
        
        for i in range(nrow):

            chk = self.uif.table_variables_a_importer.cellWidget(i, 0)
            if not chk.isChecked():
                current_var = self.uif.table_variables_a_importer.item(i, 1).text()
                self.vars_d.pop(self.vars_d.index(current_var))
        
        print('vars_d est : ', self.vars_d)

        tuple_g = self.dict_df[self.base_g]
        tuple_d = self.dict_df[self.base_d]

        df_g = self.tuple_df[tuple_g]
        df_d = self.tuple_df[tuple_d]

        if (tuple_g[0], tuple_d[0]) == (LABEL_BASE_ASSIETTE, LABEL_BASE_ALD):
            type_base = 1

        elif (tuple_g[0], tuple_d[0]) == (LABEL_BASE_CONSO, LABEL_BASE_ASSIETTE + "_" + LABEL_BASE_ALD):
            type_base = 2

        else:
            type_base = 2

        base_complete, self.nb_manquants = calculs.fusion(df_g, df_d, self.cle_g, self.cle_d, self.vars_d, type_base, self.type_jointure)
        
        if type_base == 1 and "is_ALD" in base_complete.columns:
            base_complete = base_complete.withColumn("is_ALD", F.when(F.col("is_ALD").isNull(), 0).otherwise(F.col("is_ALD")))

        if isinstance(tuple_g, tuple) and isinstance(tuple_d, tuple):
            self.tuple_df["JOINTURE : { " + tuple_g[0] + str(tuple_g[1]) + " AVEC " + tuple_d[0] + ", " + str(tuple_d[1]) + " }"] = base_complete
        else:
            self.tuple_df["JOINTURE : { "+ str(tuple_g) + " AVEC " + str(tuple_d) + " }"] = base_complete

        print(self.tuple_df)

        super().accept()

    def get_missing(self):
        return self.nb_manquants





        
