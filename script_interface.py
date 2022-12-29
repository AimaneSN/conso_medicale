
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

abspath = os.path.abspath(__file__) #Chemin absolu du script exécuté
dname = os.path.dirname(abspath) #Chemin du dossier contenant le script
os.chdir(dname)

from interface_1 import Ui_MainWindow #Importation de l'interface créée avec Qt Designer

from script_dialog_modifier import dialog_modifier
from script_dialog_traitement import dialog_traitements
from script_dialog_modalites import dialog_modalites

from dialog_modalites import Ui_Dialog

import coldict
import readfile
import utils

#Liste des libellés des bases
LABEL_BASE_ASSIETTE = "DEMOGRAPHIQUE"
LABEL_BASE_CONSO = "CONSOMMATIONS"
LABEL_BASE_ALD =  "ALD/ALC"
LABEL_BASE_MEDIC = "MEDICAMENTS"

liste_bases = [LABEL_BASE_ASSIETTE, LABEL_BASE_CONSO, LABEL_BASE_ALD, LABEL_BASE_MEDIC]

class RegExpValidator(QtGui.QRegularExpressionValidator):
    validationChanged = QtCore.pyqtSignal(QtGui.QValidator.State)

    def validate(self, input, pos):
        state, input, pos = super().validate(input, pos)
        self.validationChanged.emit(state)
        return state, input, pos

class MyTableModel(QtCore.QAbstractTableModel):   
    #Permet l'aperçu des fichiers importés

    def __init__(self, data=[[]], parent=None):
        super().__init__(parent)
        self.data = data

    def headerData(self, section: int, orientation: QtCore.Qt.Orientation, role: int):
        if role == QtCore.Qt.DisplayRole:
            if orientation == QtCore.Qt.Horizontal:
                return "Colonne " + str(section)
            else:
                return "Ligne " + str(section)

    def columnCount(self, parent=None):
        if len(self.data)>0:
            return len(self.data[0])
        else:
            return 0

    def rowCount(self, parent=None):
        return len(self.data)

    def data(self, index: QtCore.QModelIndex, role: int):
        if role == QtCore.Qt.DisplayRole:
            row = index.row()
            col = index.column()
            return str(self.data[row][col])

class base_table_widgets(QtWidgets.QWidget):
    #Permet de remplir la colonne 0 de la table des bases de données (en haut à gauche)

    def __init__(self, table, rowPos, colPos, cell_text, bttn_group, parent=None):
        super(base_table_widgets,self).__init__(parent)

        layout = QtWidgets.QHBoxLayout()

        rad_ = QtWidgets.QRadioButton()        
        bttn_group.addButton(rad_, rowPos)

        layout.addWidget(rad_)

        lb = QtWidgets.QLabel()
        lb.setText(cell_text)
        
        #table.setCellWidget(rowPos, colPos, lb)
        layout.addWidget(lb)

        #layout.setSpacing(3)
        self.setLayout(layout)

class MainWindow_(QtWidgets.QMainWindow):
    #Fenêtre principale

    def __init__(self):
        super(MainWindow_, self).__init__()

        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        self.d_dict = {} # dictionnaire des indices
        self.d_types = {} # dictionnaire des types

        self.dict_mod = {}
        self.dict_all_mods = {}

        self.dict_paths = {} # dictionnaire des chemins, {chemin : [etat, (libellé, année)]}
        self.dict_paths_ = {} #{(libelle, année) : chemin}
        self.paths_list = []

        self.format_mem = {} # { var: dict d'essais{format, status} }, permet de mémoriser les formats de dates traités 

        self.showMaximized() 

        #Lancement de la session Spark
        self.ses_sp =  SparkSession.builder.config("spark.driver.memory", "2g").appName('conso_medicale').enableHiveSupport().getOrCreate()
        self.ses_sp.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

        self.bttn_group = QtWidgets.QButtonGroup()

        self.ui.button_ajouter_base.clicked.connect(self.ajouter_base)
        self.ui.button_selectionner.clicked.connect(self.selectionner)
        self.ui.button_enregistrer.clicked.connect(self.enregistrer)
        self.ui.button_annuler.clicked.connect(self.annuler)
        self.ui.button_supprimer.clicked.connect(self.supprimer)
        self.ui.button_modifier_variables.clicked.connect(self.modifier)
        self.ui.button_calculs_traitements.clicked.connect(self.lancer_calculs)
        
        self.ui.button_selectionner.setEnabled(False)
        self.ui.button_enregistrer.setEnabled(False)
        self.ui.button_annuler.setEnabled(False)
        self.ui.button_supprimer.setEnabled(False)
        self.ui.button_modifier_variables.setEnabled(False)

    def enregistrer(self):
        #Définit ce que fait le programme quand on clique sur le bouton "Enregistrer"

        self.nrow = self.ui.variables_table.rowCount()
        self.cmb = [self.ui.variables_table.cellWidget(i,1) for i in range(0, self.nrow)] #Liste des combobox (listes déroulantes) de la colonne des variables
        self.cmb_texts_ = [widget.currentText() for widget in self.cmb] #Récupère tous les choix des listes déroulantes, y compris les choix vides (l'indice de chaque élèment de la liste sera alors égal à l'indice de la colonne, donc on utilise cette liste pour affecter les indices)
        self.cmb_texts = [self.cmb_txt for self.cmb_txt in self.cmb_texts_ if self.cmb_txt != " "] #Récupère les choix des listes déroulantes en dehors des choix vides

        #Vérifier si la même colonne est choisie plus d'une fois. Si c'est le cas, le programme affiche un message d'erreur.
        if (len(self.cmb_texts) != len(set(self.cmb_texts))):
            self.showdialog("Vous avez choisi la meme variable plus d'une fois. Veuillez modifier votre choix", "Erreur")
            return

        chkId = self.bttn_group.checkedId()

        #Récupération de la position de la base sélectionnée

        self.label_base = self.ui.bases_table.cellWidget(chkId, 2).currentText()
        self.annee = int(self.ui.bases_table.cellWidget(chkId, 3).text())
        
        self.dict_indices = {}

        if (self.label_base == LABEL_BASE_ASSIETTE):
            self.dict_indices = copy.deepcopy(coldict.assiette_indices)
            self.dict_types = copy.deepcopy(coldict.assiette_types)
            self.l_selected = [ele for ele in coldict.assiette_etat.keys() if coldict.assiette_etat[ele]==True]

        elif (self.label_base == LABEL_BASE_CONSO):
            self.dict_indices = copy.deepcopy(coldict.conso_indices)
            self.dict_types = copy.deepcopy(coldict.conso_types)
            self.l_selected = [ele for ele in coldict.conso_etat.keys() if coldict.conso_etat[ele]==True]

        elif (self.label_base == LABEL_BASE_ALD):
            self.dict_indices = copy.deepcopy(coldict.ald_indices)
            self.dict_types = copy.deepcopy(coldict.ald_types)
            self.l_selected = [ele for ele in coldict.ald_etat.keys() if coldict.ald_etat[ele]==True]

        elif (self.label_base == LABEL_BASE_MEDIC):
            self.dict_indices = copy.deepcopy(coldict.medic_indices)
            self.dict_types = copy.deepcopy(coldict.medic_types)
            self.l_selected = [ele for ele in coldict.medic_etat.keys() if coldict.medic_etat[ele]==True]
        
        #Affectation des indices

        if (len(self.l_selected) == len(self.cmb_texts)): #Vérifie que toutes les variables sont affectées à une colonne:

            self.confirmation_enregistrer = self.showdialog("Etes-vous sur de vouloir enregistrer ce choix ?", "Enregistrement")
            if not self.confirmation_enregistrer:
                return

            #Suppression des variables non utilisées
            l = list(self.dict_indices.keys())
            for k in l:
                if k not in self.l_selected:
                    self.dict_indices.pop(k)
                    self.dict_types.pop(k)
            
            year = int(self.ui.bases_table.cellWidget(chkId, 3).text())
            for i, ele in enumerate(self.cmb_texts_):
                if ele != " ":
                    self.dict_indices[ele] = i
                    self.d_dict[(self.label_base, self.annee)] = self.dict_indices #Enregistre le dictionnaire des indices dans le dictionnaire d_dict
                    self.d_types[self.label_base] = self.dict_types

                    selected_path = list(self.dict_paths)[chkId] #chemin
                    self.dict_paths[selected_path] = (True, self.label_base, year) #Affectation de l'état et l'année           

                    #Affectation des formats à dates_formats_dict
                    nrows = self.ui.variables_table.rowCount()
                    format = self.ui.variables_table.cellWidget(i,2)
                    if format and ele in coldict.dates_formats_dict.keys():
                        if format.text() != '':
                            coldict.dates_formats_dict[ele] = format.text()
                        else:
                            self.showdialog("Vous n'avez pas saisi le format de la date pour la variable "+ ele+ ". Veuillez reessayer.", "Erreur")
                            return
            
            for k, v in self.dict_paths.items():
                self.dict_paths_[(v[1], year)] = k
            
        elif (len(self.dict_indices) > len(self.cmb_texts)):
            self.showdialog("Vous n'avez pas affecté toutes les variables à une colonne. Veuillez réessayer.\nSi une variable n'est pas disponible dans la base, veuillez modifier vos choix en cliquant sur 'Modifier les variables'.", "Erreur")
            return
        
        #Inviter l'utilisateur à choisir les modalités (via dialog_modalites)
        
        vars_m = []
        for col in self.dict_indices.keys():
            if col in coldict.vars_mod.keys():
                vars_m.append(col)
        
        if vars_m != []:
            d_modalite = dialog_modalites(vars_m, selected_path, self.dict_indices)
            E = d_modalite.exec()
            if (E == d_modalite.Accepted):
                self.dict_mod[(self.label_base, self.annee)] = d_modalite.db # {base : dataframe modifié}
                self.dict_all_mods = d_modalite.dict_mods # {base : dictionnaire ds modalités}

        #Modification de l'état du fichier enregistré. ("Non enregistré" -> "Enregistré")
 
        for k in self.dict_paths.keys():
            if self.dict_paths[k][0] == False :
                self.ui.bases_table.setItem(self.paths_list.index(k), 1, QtWidgets.QTableWidgetItem("Non enregistré"))
            else:
                self.ui.bases_table.setItem(self.paths_list.index(k), 1, QtWidgets.QTableWidgetItem("Enregistré"))

        self.ui.variables_table.setRowCount(0)
        self.ui.button_enregistrer.setEnabled(False)
        self.ui.button_annuler.setEnabled(False) 
        self.ui.button_modifier_variables.setEnabled(False)

        #Suppression de l'aperçu de la table
        empty_model = MyTableModel([])
        self.ui.table_apercu.setModel(empty_model)

        self.enable_upper_left(True)
        
    def showdialog(self, text : str, title : str):
        #Fonction générique pour l'affichage de boîtes de dialogues (boutons OK+Cancel)

        msg = QtWidgets.QMessageBox()
        msg.setText(text)
        msg.setWindowTitle(title)
        msg.setStandardButtons(QtWidgets.QMessageBox.Ok | QtWidgets.QMessageBox.Cancel)
        returnValue = msg.exec()
        if returnValue == QtWidgets.QMessageBox.Ok:
            return True
        return False

    def showdialog_ok(self, text : str, title : str):
        #Fonction générique pour l'affichage de boîtes de dialogues (bouton OK)

        msg = QtWidgets.QMessageBox()
        msg.setText(text)
        msg.setWindowTitle(title)
        msg.setStandardButtons(QtWidgets.QMessageBox.Ok)
        msg.exec()

    def ajouter_base(self):
        #Définit ce que fait le programme quand on clique sur le bouton "Ajouter une base"

        #Ouverture d'une boîte de dialogue de fichier (File Dialog) permettant à l'utilisateur de choisir la base, on obtient ainsi le chemin absolu de la base
        fname = QtWidgets.QFileDialog.getOpenFileName(self.ui.centralwidget, 'Selectionnez le fichier a importer')
        if fname[0] == '':
            return
        
        if fname[0] in self.dict_paths.keys():
            self.showdialog_ok("Vous avez deja ajoute ce fichier", "Erreur")
            return

        self.dict_paths[fname[0]] = (False, " ", None)
        self.paths_list.append(fname[0])

        #Vérifie si la base n'a pas été déjà ajoutée

        self.radio_base = QtWidgets.QRadioButton()
        rowPos = self.ui.bases_table.rowCount()
        self.ui.bases_table.insertRow(rowPos)

        self.ui.bases_table.setCellWidget(rowPos,0, base_table_widgets(self.ui.bases_table,rowPos,0,fname[0],self.bttn_group))
        self.ui.bases_table.setItem(rowPos, 1, QtWidgets.QTableWidgetItem("Non enregistré"))

        #Ajout de la liste déroulante (pour le choix du type de la base)
        self.combobox_base_choisie = QtWidgets.QComboBox()
        self.combobox_base_choisie.addItems([" "] + liste_bases)
        self.ui.bases_table.setCellWidget(rowPos, 2, self.combobox_base_choisie)

        #Ajout de QLineEdit pour la saisie des années d'inventaire:
        annee_editor = QtWidgets.QLineEdit()
        self.ui.bases_table.setCellWidget(rowPos, 3, annee_editor)

        annee_validator = QtGui.QIntValidator()
        annee_editor.setValidator(annee_validator)

        self.ui.bases_table.resizeColumnsToContents()

        #Déverrouillage des boutons sélectionner et supprimer

        self.ui.button_selectionner.setEnabled(True)
        self.ui.button_supprimer.setEnabled(True)

    def selectionner(self):

        #Définit ce que fait le programme quand l'utilisateur clique sur le bouton "Sélectionner"
        
        rowCount = self.ui.bases_table.rowCount()
        if (rowCount > 0): #vérifie que l'utilisateur a ajouté au moins une base, sinon ne fait rien

            self.ui.bases_table.setCurrentCell(rowCount-1, 0)

            chkId = self.bttn_group.checkedId() # égal à -1 si l'utilisateur n'a coché aucune base.
            combo_texts = [self.ui.bases_table.cellWidget(i,2).currentText()  for i in range(0, rowCount)]
            #year = int(self.ui.bases_table.cellWidget(chkId, 3).text())
            if (chkId != -1) and (combo_texts[chkId] != " "): # Si l'utilisateur a coché une base ET choisi le type de la base, alors :
                
                #Vérification de l'année d'inventaire
                year_widget = self.ui.bases_table.cellWidget(chkId, 3)
                if year_widget.text() == "":
                    pos = year_widget.cursorRect().bottomLeft()
                    year_widget.setStyleSheet('border: 3px solid red')
                    QtWidgets.QToolTip.showText(year_widget.mapToGlobal(pos), "Veuillez saisir l'annee d'inventaire")
                    return

                year_widget.setStyleSheet('')
                year = int(year_widget.text())
                
                selectedState = self.ui.bases_table.item(chkId, 1).text()
                if "Enregistré" in selectedState:
                    confirmation_modifier = self.showdialog("La base sélectionnée a déjà été enregistrée. Etes-vous sur de vouloir continuer ?", "Confirmation")
                    if not confirmation_modifier:
                        return

                self.paths_list = list(self.dict_paths)
                self.dict_paths[self.paths_list[chkId]] = (False, year)
                self.ui.bases_table.setItem(chkId, 1, QtWidgets.QTableWidgetItem("Non enregistré"))
                
                #Création d'un aperçu de la base sélectionnée

                selectedPath = self.paths_list[chkId]
                sep = readfile.get_colnames(selectedPath)[-1]

                model=MyTableModel(readfile.get_data(selectedPath, sep, 50)) #L'apercu affiche les 50 premières lignes de la base (en-tête incluse)
                self.ui.table_apercu.setModel(model)

                #Remplissage de la table "variables_table":
                self.ui.variables_table.setRowCount(0) #Vidage de la table "variables_table"

                ##Remplissage de la colonne "colonnes" avec les noms des colonnes de la base sélectionnée
                selected_file_colnames = readfile.get_colnames(selectedPath)[:-1]

                for col_n in selected_file_colnames:
                    rowPos = self.ui.variables_table.rowCount()
                    self.ui.variables_table.insertRow(rowPos)
                    self.ui.variables_table.setItem(rowPos, 0, QtWidgets.QTableWidgetItem(col_n))

                    self.format_mem[col_n] = {} #Permet de mémoriser les formats saisis

                ##Remplissage de la colonne "variables" avec des listes déroulantes (combobox)

                l =  [" "] 

                if (combo_texts[chkId] == LABEL_BASE_ASSIETTE): 
                    l += [ele for ele in list(coldict.assiette_etat) if coldict.assiette_etat[ele] == True] #Seules les variables choisies sont ajoutées aux listes déroulantes
                elif (combo_texts[chkId] == LABEL_BASE_CONSO): 
                    #l += list(coldict.conso_indices.keys())
                    l += [ele for ele in list(coldict.conso_etat) if coldict.conso_etat[ele] == True]
                elif (combo_texts[chkId] == LABEL_BASE_ALD): 
                    l += [ele for ele in list(coldict.ald_etat) if coldict.ald_etat[ele] == True]
                elif (combo_texts[chkId] == LABEL_BASE_MEDIC):
                    l += [ele for ele in list(coldict.medic_etat) if coldict.medic_etat[ele] == True]
                
                selected_path = list(self.dict_paths)[chkId] #chemin
                
                self.dict_paths[selected_path] = [combo_texts[chkId], year] #Affectation du libellé et de l'année

                for i in range(0, len(selected_file_colnames)):
                    self.combobox_vars = QtWidgets.QComboBox()
                    self.combobox_vars.addItems(l)
                    self.ui.variables_table.setCellWidget(i, 1, self.combobox_vars)
                    self.combobox_vars.currentTextChanged.connect(self.col_format)

                self.ui.button_enregistrer.setEnabled(True)
                self.ui.button_annuler.setEnabled(True)
                self.ui.button_modifier_variables.setEnabled(True)

                self.enable_upper_left(False)

            elif (chkId != -1) and (combo_texts[chkId]  == " "): # L'utilisateur a coché une base sans avoir choisi son type
                self.showdialog ("Veuillez choisir le type de la base que vous avez cochée", "Erreur")
            else: # L'utilisateur n'a coché aucune base
                self.showdialog ("Veuillez cocher une base", "Erreur")
    
    def col_format(self, text):
        #Création d'un widget LineEdit pour la saisie des formats

        rowPos = self.ui.variables_table.currentRow()

        if text in coldict.dates_formats_dict.keys():

            self.formatEdit = QtWidgets.QLineEdit()
            self.formatEdit.setPlaceholderText("Veuillez saisir le format de la date")

            format_validator = RegExpValidator(QtCore.QRegularExpression("[^0-9abcefghijklnopqrtuvwxzABCDEFGIJKLNOPQRSTUVWXYZ]+"), self)
            format_validator.validationChanged.connect(self.handleValidationChanged)
            self.formatEdit.setValidator(format_validator)

            self.formatEdit.editingFinished.connect(self.check_date_format)
            self.ui.variables_table.setCellWidget(rowPos, 2, self.formatEdit)

        else:
            self.ui.variables_table.removeCellWidget(rowPos,2)
            self.ui.variables_table.setItem(rowPos, 2, QtWidgets.QTableWidgetItem(""))
            self.currentItem = self.ui.variables_table.item(rowPos, 2)
            self.currentItem.setFlags(self.currentItem.flags() & ~QtCore.Qt.ItemFlag.ItemIsEditable)
    
    def handleValidationChanged(self, state):
        
        rowPos = self.ui.variables_table.currentRow()
        self.edit = self.ui.variables_table.cellWidget(rowPos, 2)
        pos = self.edit.cursorRect().bottomLeft()

        if state in [QtGui.QValidator.Invalid, QtGui.QValidator.Intermediate]:
            QtWidgets.QToolTip.showText(self.edit.mapToGlobal(pos), "Caractere non autorise, seuls les caracteres : M, y, d et les separateurs sont autorises", self.edit, self.edit.cursorRect(), 1500)


    def check_date_format(self):
        #Vérification du format saisi (pour les variables du type DateType())

        chkId = self.bttn_group.checkedId()
        rowPos = self.ui.variables_table.currentRow()
        current_lineedit = self.ui.variables_table.cellWidget(rowPos, 2)

        currentpath = self.paths_list[chkId]
        current_var = self.ui.variables_table.item(rowPos, 0).text() #last format
        current_d = self.format_mem[current_var]

        format_saisi = current_lineedit.text() #Format saisi par l'utilisateur

        self.date_obj = self.ui.variables_table.cellWidget(rowPos, 2)
        self.pos = self.date_obj.cursorRect().bottomLeft()
         
        if format_saisi not in current_d.keys(): #Si le format saisi est déjà traité, on ne refait pas le traitement
            dates = readfile.get_col_firstlines(currentpath, current_var, 50)
            
            l_dates = self.ses_sp.createDataFrame(dates, StringType()) \
                          .withColumn("value", F.to_date(F.to_timestamp(F.trim("value"), format_saisi))) \
                          .select("value").rdd.flatMap(lambda x: x).collect() #liste de dates sur lesquelles on a tenté la conversion avec le format saisi par l'utilisateur

            if set(l_dates) == {None}: current_d[format_saisi] = 0
            elif set(l_dates) != {None} and None in set(l_dates): current_d[format_saisi] = 1
            else: current_d[format_saisi] = 2

        if current_d[format_saisi] == 0: #La conversion a échoué, format invalide (toutes les dates sont converties en None)
            self.date_obj.setStyleSheet('border: 3px solid red')
            QtWidgets.QToolTip.showText(self.date_obj.mapToGlobal(self.pos), "Format incorrect!")
            return

        if current_d[format_saisi] == 1: #La conversion a réussi mais pas pour toutes les dates (certaines dates sont converties en None)
            self.date_obj.setStyleSheet('border: 3px solid yellow')
            QtWidgets.QToolTip.showText(self.date_obj.mapToGlobal(self.pos), "Certaines dates ne sont pas converties correctement")

        if current_d[format_saisi] == 2:  #La conversion a réussi pour toutes les dates (aucune date n'est convertie en None)
            self.date_obj.setStyleSheet("border: 3px solid lime")
            QtWidgets.QToolTip.showText(self.date_obj.mapToGlobal(self.pos), "Format correct")         
    
    def annuler(self):
        #Définit ce que fait le programme quand on clique sur le bouton "Annuler"

        confirmation_annuler = self.showdialog("Etes-vous sur de vouloir annuler l'operation? Les variables saisies seront perdues", "Confirmation")
        if not confirmation_annuler:
            return

        self.ui.variables_table.setRowCount(0)

        #Suppression de l'aperçu de la table
        empty_model = MyTableModel([])
        self.ui.table_apercu.setModel(empty_model)

        self.ui.button_enregistrer.setEnabled(False)
        self.ui.button_annuler.setEnabled(False)
        self.ui.button_modifier_variables.setEnabled(False)

        self.enable_upper_left(True)

    def supprimer(self):

        #Vérifier que l'utilisateur a coché une base à supprimer
        chkId = self.bttn_group.checkedId()
        if chkId == -1:
            self.showdialog("Aucune base à supprimer n'a été cochée", "Erreur")
            return

        confirmation_supprimer = self.showdialog("Etes-vous sur de vouloir supprimer ce fichier ?", "Confirmation")
        if not confirmation_supprimer:
            return

        #Suppression de la ligne sélectionnée
        self.ui.bases_table.removeRow(chkId)
        #Suppression du chemin sélectionné
        selectedPath = list(self.dict_paths.keys())[chkId]
        
        self.dict_paths.pop(selectedPath)
        self.paths_list.remove(selectedPath)
        nrow = self.ui.bases_table.rowCount()

        if (nrow == 0): #Les boutons "supprimer" et "sélectionner" sont verrouillés quand il n'y a plus de ligne à supprimer/sélectionner
            self.ui.button_supprimer.setEnabled(False)
            self.ui.button_selectionner.setEnabled(False)

    def modifier(self, parent = None):

        chkId = self.bttn_group.checkedId()
        label = self.ui.bases_table.cellWidget(chkId, 2).currentText()

        d_modifier = dialog_modifier(label, self.bttn_group, self.ui.bases_table)

        E = d_modifier.exec()

        if (E == d_modifier.Accepted):
           self.selectionner()

    def enable_upper_left(self, State : bool):
        #Permet d'activer/désactiver la partie en haut à gauche de l'interface

        self.ui.button_selectionner.setEnabled(State)
        self.ui.button_ajouter_base.setEnabled(State)
        self.ui.button_supprimer.setEnabled(State)

        for i, button in enumerate(self.bttn_group.buttons()):
                    button.setEnabled(State)
                    self.ui.bases_table.cellWidget(i,2).setEnabled(State)
                    self.ui.bases_table.cellWidget(i,3).setEnabled(State)
                    
    def get_indexes(self):
        return self.d_dict

    def lancer_calculs(self):
            d_traitement = dialog_traitements(self.d_dict, self.d_types, self.dict_paths_, self.dict_mod, self.dict_all_mods)
            d_traitement.exec()

if __name__ == '__main__':

    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow_()
    win.show()
    sys.exit(app.exec_())
