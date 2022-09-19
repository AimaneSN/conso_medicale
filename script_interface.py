from venv import EnvBuilder
from PyQt5 import QtCore, QtGui, QtWidgets

import sys 
import os
os.chdir("/home/aimane/Desktop/pyzo_")

import coldict
import readfile

LABEL_BASE_ASSIETTE = "DEMOGRAPHIQUE"
LABEL_BASE_CONSO = "CONSO"
LABEL_BASE_ALD =  "ALD/ALC"

from interface_2 import Ui_MainWindow #Importation de l'interface créée sous Qt Designer

class MyTableModel(QtCore.QAbstractTableModel): 
    #Classe pour l'aperçu des bases de données ajoutées
    
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
        return len(self.data[0])

    def rowCount(self, parent=None):
        return len(self.data)

    def data(self, index: QtCore.QModelIndex, role: int):
        if role == QtCore.Qt.DisplayRole:
            row = index.row()
            col = index.column()
            return str(self.data[row][col])

class base_table_widgets(QtWidgets.QWidget):
    #Classe pour le remplissage de la table des bases de données ajoutées
    
    def __init__(self, table, rowPos, colPos, cell_text, bttn_group, parent=None):
        super(base_table_widgets,self).__init__(parent)

        layout = QtWidgets.QHBoxLayout()
        
        # widgets à ajouter à la cellule
        rad_ = QtWidgets.QRadioButton()

        layout.addWidget(rad_)
        bttn_group.addButton(rad_, rowPos)

        table.setItem(rowPos, colPos, QtWidgets.QTableWidgetItem(cell_text))

        self.setLayout(layout)


class MainWindow_(QtWidgets.QMainWindow):
    #Classe de la fenêtre principale de l'interface
    
    def __init__(self):
        super(MainWindow_, self).__init__()

        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        d_dict = {}

        bttn_group = QtWidgets.QButtonGroup()

        def enregistrer():
            #définit le comportement du bouton "Enregistrer"
            
            nrow = self.ui.variables_table.rowCount()
            cmb = [self.ui.variables_table.cellWidget(i,1) for i in range(0, nrow)] #Liste des combobox (listes déroulantes) de la colonne des variables
            cmb_texts_ = [widget.currentText() for widget in cmb] #Récupère tous les choix des listes déroulantes
            
            cmb_texts = [cmb_txt for cmb_txt in cmb_texts_ if cmb_txt != " "] #Récupère les choix des listes déroulantes en dehors du choix par défaut (aucune variable)

            #Vérifier si la même colonne est choisie plus d'une fois. Si c'est le cas, le programme affiche un message d'erreur.
            if (len(cmb_texts) != len(set(cmb_texts))):
                showdialog("Vous avez choisi la meme variable plus d'une fois. Veuillez modifier votre choix", "Erreur")
                return

            dict = {}

            #Récupération de la position de la base sélectionnée
            nrow_ = self.ui.bases_table.rowCount()
            etats = [self.ui.bases_table.item(i,1).text() for i in range(0, nrow_)]
            rowPos = etats.index("Sélectionné")

            label_base = self.ui.bases_table.cellWidget(rowPos,2).currentText()

            if (label_base == LABEL_BASE_ASSIETTE):
                dict = coldict.assiette_indices
            elif (label_base == LABEL_BASE_CONSO):
                dict = coldict.conso_indices
            elif (label_base == LABEL_BASE_ALD):
                dict = coldict.ald_indices

            #Enregistrement des indices choisis

            if (len(dict) == len(cmb_texts)): #Vérifie que toutes les variables sont affectées à une colonne:
                    for i, ele in enumerate(cmb_texts_):
                        if ele != " ":
                            dict[ele] = i
                    d_dict[label_base] = dict #Enregistre le dictionnaire des indices dans le dictionnaire d_dict
        
            elif (len(dict) > len(cmb_texts)):
                showdialog("Vous n'avez pas rentré toutes les colonnes. Veuillez réessayer", "Erreur")
                return
            
            confirmation_enregistrer = showdialog("Etes-vous sur de vouloir enregistrer ce choix ?", "Enregistrement")
            if not confirmation_enregistrer:
                return
            
            self.ui.variables_table.setRowCount(0)
            self.ui.button_enregistrer.setEnabled(False)
            self.ui.button_annuler.setEnabled(False)

            showdialog (str(dict) + "\n"+ str(d_dict), "resultat")  

            enable_upper_left(True)
        
        def showdialog(text : str, title : str):
            #Fonction générique pour l'affichage de boîtes de dialogue
            
            msg = QtWidgets.QMessageBox()
            msg.setText(text)
            msg.setWindowTitle(title)
            msg.setStandardButtons(QtWidgets.QMessageBox.Ok | QtWidgets.QMessageBox.Cancel)
            returnValue = msg.exec()
            if returnValue == QtWidgets.QMessageBox.Ok:
                return True
            return False

        def ajouter_base():
            #définit le comportement du bouton "Ajouter une base"
            
            fname = QtWidgets.QFileDialog.getOpenFileName(self.ui.centralwidget, 'Selectionnez le fichier a importer')
            if (fname[0] == ''):
                return

            nrow = self.ui.bases_table.rowCount()

            #Vérifie si la base n'a pas été déjà ajoutée
            if nrow > 0:
                
                paths = [self.ui.bases_table.item(i,0).text() for i in range(0, nrow)] #liste des chemins déjà ajoutés par l'utilisateur

                if fname[0] in paths: #si le chemin qu'on vient d'ajouter appartient déjà à la liste
                    dial = showdialog("Vous avez déjà ajouté ce fichier. Êtes-vous sûr de vouloir continuer ?", "Erreur")
                    if not dial: #L'utilisateur a cliqué sur le bouton Annuler/Cancel
                        return

            self.radio_base = QtWidgets.QRadioButton()
            rowPos = self.ui.bases_table.rowCount()
            self.ui.bases_table.insertRow(rowPos)

            self.ui.bases_table.setCellWidget(rowPos,0, base_table_widgets(self.ui.bases_table,rowPos,0,fname[0],bttn_group))
            self.ui.bases_table.setItem(rowPos, 1, QtWidgets.QTableWidgetItem("Non sélectionné"))

            #Ajout de la liste déroulante (pour le choix du type de la base)
            self.combobox_base_choisie = QtWidgets.QComboBox()
            self.combobox_base_choisie.addItems([" ", LABEL_BASE_ASSIETTE, LABEL_BASE_CONSO, LABEL_BASE_ALD])
            self.ui.bases_table.setCellWidget(rowPos, 2, self.combobox_base_choisie)

            self.ui.button_selectionner.setEnabled(True) 
            self.ui.button_supprimer.setEnabled(True)

        def selectionner():
            #définit le comportement du bouton "Sélectionner", celui-ci ne fait rien tant que l'utilisateur n'aura pas ajouté, coché une base et choisi son type
            
            rowCount = self.ui.bases_table.rowCount()
            if (rowCount > 0): #vérifie que l'utilisateur a ajouté au moins une base, sinon ne fait rien
            
                self.ui.bases_table.setCurrentCell(rowCount-1, 0)

                chkId = bttn_group.checkedId() # égal à -1 si l'utilisateur n'a coché aucune base.
                combo_texts = [self.ui.bases_table.cellWidget(i,2).currentText()  for i in range(0, rowCount)]

                if (chkId != -1) and (combo_texts[chkId] != " "): # Si l'utilisateur a coché une base ET choisi le type de la base, alors :

                    #Modification de l'état de la base choisie de "Non sélectionné" à "Sélectionné"
                    for i in range(0, self.ui.bases_table.rowCount()):
                        self.ui.bases_table.setItem(i, 1, QtWidgets.QTableWidgetItem("Non sélectionné")) ## Mettre l'état de toutes les bases en "Non sélectionné"

                    selectedPath=self.ui.bases_table.item(chkId,0).text()
                    self.ui.bases_table.setItem(chkId, 1, QtWidgets.QTableWidgetItem("Sélectionné"))  ## Mettre l'état de la base choisie en "Sélectionné"

                    #Création d'un aperçu de la base sélectionnée

                    model=MyTableModel(readfile.get_data(selectedPath, ";", 50)) #L'apercu affiche les 50 premières lignes de la base (en-tête incluse)
                    self.ui.table_apercu.setModel(model)
                    
                    #Remplissage de la table "variables_table":
                    self.ui.variables_table.setRowCount(0) #Vidage de la table "variables_table"
                
                    ##Remplissage de la colonne "colonnes" avec les noms des colonnes de la base sélectionnée
                    selected_file_colnames = readfile.get_colnames(selectedPath)[:-1]

                    for col_n in selected_file_colnames:
                        rowPos = self.ui.variables_table.rowCount()
                        self.ui.variables_table.insertRow(rowPos)
                        self.ui.variables_table.setItem(rowPos, 0, QtWidgets.QTableWidgetItem(col_n))

                    ##Remplissage de la colonne "variables" avec des listes déroulantes (combobox)

                    l =  [" "] # Choix par défaut de la liste déroulante (aucune base choisie)
                    if (combo_texts[chkId] == LABEL_BASE_ASSIETTE): # Si le type de base en question est la base démographique
                        l += list(coldict.assiette_indices.keys()) # Remplir la liste déroulante avec les variables de la base démographique dont on aura besoin
                    elif (combo_texts[chkId] == LABEL_BASE_CONSO): # Si c'est la base des consommations
                        l += list(coldict.conso_indices.keys())
                    elif (combo_texts[chkId] == LABEL_BASE_ALD): # Si c'est la base ALD/ALC
                        l += list(coldict.ald_indices.keys())

                    for i in range(0, len(selected_file_colnames)):
                        self.combobox_vars = QtWidgets.QComboBox()
                        self.combobox_vars.addItems(l)
                        self.ui.variables_table.setCellWidget(i,1,self.combobox_vars)

                    self.ui.button_enregistrer.setEnabled(True)
                    self.ui.button_annuler.setEnabled(True)
                    enable_upper_left(False)
                        
    
                elif (chkId != -1) and (combo_texts[chkId]  == " "): # L'utilisateur a coché une base sans avoir choisi son type
                    showdialog ("Veuillez choisir le type de la base que vous avez cochée", "Erreur")
                else: # L'utilisateur n'a coché aucune base
                    showdialog ("Veuillez cocher une base", "Erreur")
        
        def annuler():
            #définit le comportement du bouton "Annuler"
            
            confirmation_annuler = showdialog("Etes-vous sur de vouloir annuler l'operation? Les variables saisies seront perdues", "Confirmation")
            if not confirmation_annuler:
                return
            
            enable_upper_left(True)

        def supprimer():
            #définit le comportement du bouton "Supprimer"
            
            chkId = bttn_group.checkedId()
            if chkId == -1:
                showdialog("Aucune base à supprimer n'a été cochée", "Erreur")
                return
            
            confirmation_supprimer = showdialog("Etes-vous sur de vouloir supprimer ce fichier ?", "Confirmation")
            if not confirmation_supprimer:
                return
            
            self.ui.bases_table.removeRow(chkId)

            nrow = self.ui.bases_table.rowCount()
            if (nrow ==0):
                self.ui.button_supprimer.setEnabled(False)
                self.ui.button_selectionner.setEnabled(False)

        def enable_upper_left(State : bool):
            #Permet l'activation/la désactivation de la partie en haut à gauche de l'interface
            
            self.ui.button_selectionner.setEnabled(State)
            self.ui.button_ajouter_base.setEnabled(State)
            self.ui.button_supprimer.setEnabled(State)

            for i, button in enumerate(bttn_group.buttons()):
                        button.setEnabled(State)
                        self.ui.bases_table.cellWidget(i,2).setEnabled(State)

        self.ui.button_ajouter_base.clicked.connect(ajouter_base)
        self.ui.button_selectionner.clicked.connect(selectionner)
        self.ui.button_enregistrer.clicked.connect(enregistrer)
        self.ui.button_annuler.clicked.connect(annuler)
        self.ui.button_supprimer.clicked.connect(supprimer)

        self.ui.button_selectionner.setEnabled(False)
        self.ui.button_enregistrer.setEnabled(False)
        self.ui.button_annuler.setEnabled(False)
        self.ui.button_supprimer.setEnabled(False)

if __name__ == '__main__':

    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow_()
    win.show()
    sys.exit(app.exec_())

