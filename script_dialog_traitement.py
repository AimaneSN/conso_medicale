
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
from dialog_modalites import Ui_Dialog

import coldict
import readfile
import utils

class dialog_modalites(QtWidgets.QDialog):
    #Boite de dialogue pour récupérer les modalités pour les variables catégoriques
    def __init__(self, vars_m, currentpath, dict_indices, parent = None):
        super(dialog_modalites, self).__init__(parent)

        self.ses_sp = SparkSession.builder.config("spark.driver.memory", "2g").appName('mods').enableHiveSupport().getOrCreate()

        self.setModal(True)
        self.uim = Ui_Dialog()
        self.uim.setupUi(self)  
        self.modchanged = False
        self.currentpath = currentpath  #currentpath envoyée par MainWindow_ devient un attribut de cette classe, qu'on pourra utiliser en dehors de __init__
        self.sep = readfile.get_colnames(self.currentpath)[-1]
        self.dict_indices = dict_indices
        self.dict_mods = {} #Dictionnaire des modalités choisies par l'utilisateur

        self.uim.combo_variables.addItems([" "] + vars_m)

        self.uim.button_select_var.clicked.connect(self.selectionner_var)
        self.uim.button_enregistrer_var.clicked.connect(self.enregistrer_var)
        self.uim.button_terminer_mod.clicked.connect(self.accept)
        self.uim.button_quitter_mod.clicked.connect(self.reject)
        
        self.uim.button_enregistrer_var.setEnabled(False)
        self.uim.button_annuler_var.setEnabled(False)

    def selectionner_var(self):

        self.selectedVar = self.uim.combo_variables.currentText()
        if self.selectedVar == " ":
            self.showdialog("Vous n'avez choisi aucune variable, veuillez réessayer.", "Erreur")
            return
        
        self.db = readfile.get_file(self.currentpath, self.sep, self.ses_sp)
        self.db = utils.rename_columns(self.db, self.dict_indices)

        self.foundMods = self.db.select(self.selectedVar).distinct().rdd.flatMap(lambda x: x).collect() #liste des modalités trouvées dans la base
        selectedMods = list(coldict.vars_mod[self.selectedVar]) #liste des modalités par défaut 

        #Remplissage de la table "table_modalites"

        self.uim.table_modalites.setRowCount(0)

        for mod in self.foundMods:
            rowPos = self.uim.table_modalites.rowCount()
            self.uim.table_modalites.insertRow(rowPos)
            self.uim.table_modalites.setItem(rowPos, 0, QtWidgets.QTableWidgetItem(mod))

            modcombo = QtWidgets.QComboBox()
            modcombo.addItems([" "] + selectedMods)
            self.uim.table_modalites.setCellWidget(rowPos, 1, modcombo)
        
        self.uim.button_select_var.setEnabled(False)
        self.uim.button_enregistrer_var.setEnabled(True)
        self.uim.button_annuler_var.setEnabled(True)
        
    def enregistrer_var(self):
        #Enregistrement des modalités choisies dans le dictionnaire dict_mods

        found_dict = dict.fromkeys(self.foundMods) #Dictionnaire des modalités trouvées dans la base
        current_dict = coldict.vars_mod[self.selectedVar]
        nrow = self.uim.table_modalites.rowCount()
        l_choix = [self.uim.table_modalites.cellWidget(i, 1).currentText() for i in range(0, nrow)]
        
        if set(l_choix) != " ":
            self.modchanged = True

        for i, ele in enumerate(l_choix):
            if ele != " ":
                found_dict[list(found_dict)[i]] = current_dict[ele]

        self.dict_mods[self.selectedVar] = found_dict
        print(self.dict_mods)

        self.uim.table_modalites.setRowCount(0)
        self.uim.button_select_var.setEnabled(True)
    
    def accept(self):
        #Renommage des modalités dans la base de données choisie

        for var in self.dict_mods.keys():
            for mod in self.dict_mods[var].keys():
                if self.dict_mods[var][mod] != None:
                    self.db = self.db.withColumn(var, F.when(self.db[var] == mod, self.dict_mods[var][mod]).otherwise(self.db[var]))

        super().accept()
        
    def showdialog(self, text : str, title : str):
        #Fonction générique pour l'affichage de boîtes de dialogues

        msg = QtWidgets.QMessageBox()
        msg.setText(text)
        msg.setWindowTitle(title)
        msg.setStandardButtons(QtWidgets.QMessageBox.Ok | QtWidgets.QMessageBox.Cancel)
        returnValue = msg.exec()
        if returnValue == QtWidgets.QMessageBox.Ok:
            return True
        return False
