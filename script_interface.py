from PyQt5 import QtCore, QtGui, QtWidgets

import sys 
import os
os.chdir("/home/aimane/Desktop/pyzo_")

import coldict
import readfile

LABEL_BASE_ASSIETTE = "DEMOGRAPHIQUE"
LABEL_BASE_CONSO = "CONSO"
LABEL_BASE_ALD =  "ALD/ALC"

from interface_2 import Ui_MainWindow

class MyTableModel(QtCore.QAbstractTableModel):
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

    def __init__(self):
        super(MainWindow_, self).__init__()

        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        d_dict = {}

        bttn_group = QtWidgets.QButtonGroup()


        def enregistrer():
            nrow = self.ui.variables_table.rowCount()
            cmb = [self.ui.variables_table.cellWidget(i,1) for i in range(0, nrow)] #Liste des combobox (listes déroulantes) de la colonne des variables
            cmb_texts_ = [widget.currentText() for widget in cmb] #Récupère tous les choix des listes déroulantes
            
            cmb_texts = [cmb_txt for cmb_txt in cmb_texts_ if cmb_txt != " "] #Récupère les choix des listes déroulantes en dehors du choix par défaut (aucune variable)

            #Vérifier si la même colonne est choisie plus d'une fois. Si c'est le cas, le programme affiche un message d'erreur.
            if (len(cmb_texts) != len(set(cmb_texts))):
                showdialog("Vous avez choisi la meme variable deux fois. Veuillez modifier votre choix", "Erreur")
                return

            dict = {}
            rowPos = self.ui.bases_table.currentRow()
            label_base = self.ui.bases_table.cellWidget(rowPos,2).currentText()

            if (label_base == LABEL_BASE_ASSIETTE):
                dict = coldict.assiette_indices
            elif (label_base == LABEL_BASE_CONSO):
                dict = coldict.conso_indices
            elif (label_base == LABEL_BASE_ALD):
                dict = coldict.ald_indices

            #Affecter les indices

            if (len(dict) == len(cmb_texts)): #Vérifie que toutes les variables sont affectées à une colonne:
                    for i, ele in enumerate(cmb_texts_):
                        if ele != " ":
                            dict[ele] = i
                    d_dict[label_base] = dict #Enregistre le dictionnaire des indices dans le dictionnaire d_dict
        
            elif (len(dict) > len(cmb_texts)):
                showdialog("Vous n'avez pas rentré toutes les colonnes. Veuillez réessayer", "Erreur")
                return

            showdialog (str(dict) + "\n"+ str(d_dict), "resultat")  
        
        def showdialog(text : str, title : str):
            msg = QtWidgets.QMessageBox()
            msg.setText(text)
            msg.setWindowTitle(title)
            msg.setStandardButtons(QtWidgets.QMessageBox.Ok | QtWidgets.QMessageBox.Cancel)
            returnValue = msg.exec()
            if returnValue == QtWidgets.QMessageBox.Ok:
                return True
            return False

        def ajouter_base():
            fname = QtWidgets.QFileDialog.getOpenFileName(self.ui.centralwidget, 'Selectionnez le fichier a importer')
            if (fname[0] == ''):
                return

            nrow = self.ui.bases_table.rowCount()

            #Vérifie si la base n'a pas été déjà ajoutée
            if nrow > 0:
                
                paths_ = [self.ui.bases_table.item(i,0) for i in range(0, nrow)]
                paths = [ele.text() for ele in paths_] #liste des chemins déjà ajoutés par l'utilisateur

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

            #Déblocage du bouton sélectionner
            self.ui.button_selectionner.setEnabled(True) 

        def selectionner():
            #définit le comportement du bouton "Sélectionner", celui-ci ne fait rien tant que l'utilisateur n'aura pas ajouté, coché une base et choisi son type
            rowCount = self.ui.bases_table.rowCount()
            if (rowCount > 0): #vérifie que l'utilisateur a ajouté au moins une base, sinon ne fait rien
            
                self.ui.bases_table.setCurrentCell(rowCount-1, 0)

                chkId = bttn_group.checkedId() # égal à -1 si l'utilisateur n'a coché aucune base.
                rowPos = self.ui.bases_table.currentRow()
                combo_texts = [self.ui.bases_table.cellWidget(i,2).currentText()  for i in range(0, rowPos+1)]

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
                
                    ##Remplissage de la colonne "colonnes"
                    selected_file_colnames = readfile.get_colnames(selectedPath)[:-1]

                    for col_n in selected_file_colnames:
                        rowPos = self.ui.variables_table.rowCount()
                        self.ui.variables_table.insertRow(rowPos)
                        self.ui.variables_table.setItem(rowPos, 0, QtWidgets.QTableWidgetItem(col_n))

                    ##Remplissage de la colonne "variables"

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
    
                elif (chkId != -1) and (combo_texts[chkId]  == " "): # L'utilisateur a coché une base sans avoir choisi son type
                    showdialog ("Veuillez choisir le type de la base que vous avez cochée", "Erreur")
                else: # L'utilisateur n'a coché aucune base
                    showdialog ("Veuillez cocher une base", "Erreur")
        
        self.ui.button_ajouter_base.clicked.connect(ajouter_base)

        self.ui.button_selectionner.setEnabled(False)
        self.ui.button_selectionner.clicked.connect(selectionner)

        self.ui.button_enregistrer.clicked.connect(enregistrer)

if __name__ == '__main__':

    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow_()
    win.show()
    sys.exit(app.exec_())

