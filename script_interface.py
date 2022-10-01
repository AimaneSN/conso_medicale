from PyQt5 import QtCore, QtGui, QtWidgets

import sys 
import os


from sklearn.metrics import fbeta_score

abspath = os.path.abspath(__file__) #Chemin absolu du script exécuté
dname = os.path.dirname(abspath) #Chemin du dossier contenant le script
os.chdir(dname)

import coldict
import readfile

LABEL_BASE_ASSIETTE = "DEMOGRAPHIQUE"
LABEL_BASE_CONSO = "CONSO"
LABEL_BASE_ALD =  "ALD/ALC"

from interface_1 import Ui_MainWindow #Importation de l'interface créée avec Qt Designer

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
    
    #Permet de remplir la table des bases de données
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
    #Fenêtre principale

    def __init__(self):
        super(MainWindow_, self).__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        self.d_dict = {}

        self.dict_paths = {}
        self.paths_list = []

        self.bttn_group = QtWidgets.QButtonGroup()

        self.ui.button_ajouter_base.clicked.connect(self.ajouter_base)
        self.ui.button_selectionner.clicked.connect(self.selectionner)
        self.ui.button_enregistrer.clicked.connect(self.enregistrer)
        self.ui.button_annuler.clicked.connect(self.annuler)
        self.ui.button_supprimer.clicked.connect(self.supprimer)
        
        self.ui.button_selectionner.setEnabled(False)
        self.ui.button_enregistrer.setEnabled(False)
        self.ui.button_annuler.setEnabled(False)
        self.ui.button_supprimer.setEnabled(False)
        
    def enregistrer(self):

        #Définit ce que fait le programme quand on clique sur le bouton "Enregistrer"

        self.nrow = self.ui.variables_table.rowCount()
        self.cmb = [self.ui.variables_table.cellWidget(i,1) for i in range(0, self.nrow)] #Liste des combobox (listes déroulantes) de la colonne des variables
        self.cmb_texts_ = [widget.currentText() for widget in self.cmb] #Récupère tous les choix des listes déroulantes

        self.cmb_texts = [self.cmb_txt for self.cmb_txt in self.cmb_texts_ if self.cmb_txt != " "] #Récupère les choix des listes déroulantes en dehors du choix par défaut (aucune variable)

        #Vérifier si la même colonne est choisie plus d'une fois. Si c'est le cas, le programme affiche un message d'erreur.
        if (len(self.cmb_texts) != len(set(self.cmb_texts))):
            self.showdialog("Vous avez choisi la meme variable plus d'une fois. Veuillez modifier votre choix", "Erreur")
            return

        self.dict_indices = {}

        chkId = self.bttn_group.checkedId()


        #Récupération de la position de la base sélectionnée
        self.nrow_ = self.ui.bases_table.rowCount()
        self.etats = [self.ui.bases_table.item(i,1).text() for i in range(0, self.nrow_)]
        #self.rowPos = self.etats.index("Sélectionné")

        self.label_base = self.ui.bases_table.cellWidget(chkId,2).currentText()

        if (self.label_base == LABEL_BASE_ASSIETTE):
            self.dict_indices = coldict.assiette_indices
        elif (self.label_base == LABEL_BASE_CONSO):
            self.dict_indices = coldict.conso_indices
        elif (self.label_base == LABEL_BASE_ALD):
            self.dict_indices = coldict.ald_indices

        #Affectation des indices

        if (len(self.dict_indices) == len(self.cmb_texts)): #Vérifie que toutes les variables sont affectées à une colonne:
                for i, ele in enumerate(self.cmb_texts_):
                    if ele != " ":
                        self.dict_indices[ele] = i
                self.d_dict[self.label_base] = self.dict_indices #Enregistre le dictionnaire des indices dans le dictionnaire d_dict

                selected_path = list(self.dict_paths)[chkId] #chemin
                self.dict_paths[selected_path][0] = True #Affectation de l'état

                print(self.dict_paths)

        elif (len(self.dict_indices) > len(self.cmb_texts)):
            self.showdialog("Vous n'avez pas affecté toutes les variables à une colonne. Veuillez réessayer", "Erreur")
            return

        self.confirmation_enregistrer = self.showdialog("Etes-vous sur de vouloir enregistrer ce choix ?", "Enregistrement")
        if not self.confirmation_enregistrer:
            return
        
        self.paths_list = list(self.dict_paths)

        for k, v in self.dict_paths.items():
            if self.dict_paths[k][0] == False :
                self.ui.bases_table.setItem(self.paths_list.index(k), 1, QtWidgets.QTableWidgetItem("Non enregistré"))
            else:
                self.ui.bases_table.setItem(self.paths_list.index(k), 1, QtWidgets.QTableWidgetItem("Enregistré"))

        #self.ui.bases_table.setItem(self.rowPos, 1, QtWidgets.QTableWidgetItem("Enregistré ( "+self.label_base+" )"))

        self.ui.variables_table.setRowCount(0)
        self.ui.button_enregistrer.setEnabled(False)
        self.ui.button_annuler.setEnabled(False) 

        self.showdialog (str(self.d_dict), "resultat")

        self.enable_upper_left(True)
        
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

    def showdialog_ok(self, text : str, title : str):
        #Fonction générique pour l'affichage de boîtes de dialogues

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

        self.dict_paths[fname[0]] = [False, " "]

        #Vérifie si la base n'a pas été déjà ajoutée

        self.radio_base = QtWidgets.QRadioButton()
        rowPos = self.ui.bases_table.rowCount()
        self.ui.bases_table.insertRow(rowPos)

        self.ui.bases_table.setCellWidget(rowPos,0, base_table_widgets(self.ui.bases_table,rowPos,0,fname[0],self.bttn_group))
        self.ui.bases_table.setItem(rowPos, 1, QtWidgets.QTableWidgetItem("Non enregistre"))

        #Ajout de la liste déroulante (pour le choix du type de la base)

        self.combobox_base_choisie = QtWidgets.QComboBox()
        self.combobox_base_choisie.addItems([" ", LABEL_BASE_ASSIETTE, LABEL_BASE_CONSO, LABEL_BASE_ALD])
        self.ui.bases_table.setCellWidget(rowPos, 2, self.combobox_base_choisie)

        #Déverrouillage des boutons sélectionner et supprimer

        self.ui.button_selectionner.setEnabled(True)
        self.ui.button_supprimer.setEnabled(True)
        
        #nrow = self.ui.bases_table.rowCount()

        #paths = [self.ui.bases_table.item(i,0).text() for i in range(0, nrow)]
        #print(self.list_paths)

        #if nrow > 0:

        #    paths = [self.ui.bases_table.item(i,0).text() for i in range(0, nrow)] #liste des chemins déjà ajoutés par l'utilisateur
        #    if fname[0] in paths: #si le chemin qu'on vient d'ajouter appartient déjà à la liste "paths"
        #        dial = self.showdialog("Vous avez déjà ajouté ce fichier. Êtes-vous sûr de vouloir continuer ?", "Erreur")
        #        if not dial: #L'utilisateur a cliqué sur le bouton Annuler/Cancel
        #            return
        

    def selectionner(self):

        #Définit ce que fait le programme quand l'utilisateur clique sur le bouton "Sélectionner"
        
        rowCount = self.ui.bases_table.rowCount()
        if (rowCount > 0): #vérifie que l'utilisateur a ajouté au moins une base, sinon ne fait rien

            self.ui.bases_table.setCurrentCell(rowCount-1, 0)

            chkId = self.bttn_group.checkedId() # égal à -1 si l'utilisateur n'a coché aucune base.
            combo_texts = [self.ui.bases_table.cellWidget(i,2).currentText()  for i in range(0, rowCount)]

            if (chkId != -1) and (combo_texts[chkId] != " "): # Si l'utilisateur a coché une base ET choisi le type de la base, alors :

                selectedState = self.ui.bases_table.item(chkId, 1).text()
                if "Enregistré" in selectedState:
                    confirmation_modifier = self.showdialog("La base sélectionnée a déjà été enregistrée. Etes-vous sur de vouloir continuer ?", "Confirmation")
                    if not confirmation_modifier:
                        return

                self.paths_list = list(self.dict_paths)
                self.dict_paths[self.paths_list[chkId]][0] = False
                self.ui.bases_table.setItem(chkId, 1, QtWidgets.QTableWidgetItem("Non enregistré"))

                #Modification de l'état de la base choisie de "Non sélectionné" à "Sélectionné"
                #for i in range(0, self.ui.bases_table.rowCount()):
                #    if self.ui.bases_table.item(chkId, 0).text()
                #    self.ui.bases_table.setItem(i, 1, QtWidgets.QTableWidgetItem("Non sélectionné")) ## Mettre l'état de toutes les bases en "Non sélectionné"
                

                selectedPath=self.ui.bases_table.item(chkId, 0).text()
                #self.ui.bases_table.setItem(chkId, 1, QtWidgets.QTableWidgetItem("Sélectionné"))  ## Mettre l'état de la base choisie en "Sélectionné"

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
                
                selected_path = list(self.dict_paths)[chkId] #chemin
                self.dict_paths[selected_path][1] = combo_texts[chkId] #Affectation du libellé

                for i in range(0, len(selected_file_colnames)):
                    self.combobox_vars = QtWidgets.QComboBox()
                    self.combobox_vars.addItems(l)
                    self.ui.variables_table.setCellWidget(i,1,self.combobox_vars)

                self.ui.button_enregistrer.setEnabled(True)
                self.ui.button_annuler.setEnabled(True)
                self.enable_upper_left(False)

            elif (chkId != -1) and (combo_texts[chkId]  == " "): # L'utilisateur a coché une base sans avoir choisi son type
                self.showdialog ("Veuillez choisir le type de la base que vous avez cochée", "Erreur")
            else: # L'utilisateur n'a coché aucune base
                self.showdialog ("Veuillez cocher une base", "Erreur")

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

        self.ui.bases_table.removeRow(chkId)

        nrow = self.ui.bases_table.rowCount()
        if (nrow ==0): #Les boutons "supprimer" et "sélectionner" sont verrouillés quand il n'y a plus de ligne à supprimer/sélectionner
            self.ui.button_supprimer.setEnabled(False)
            self.ui.button_selectionner.setEnabled(False)

    def enable_upper_left(self, State : bool):

        #Permet d'activer/désactiver la partie en haut à gauche de l'interface

        self.ui.button_selectionner.setEnabled(State)
        self.ui.button_ajouter_base.setEnabled(State)
        self.ui.button_supprimer.setEnabled(State)

        for i, button in enumerate(self.bttn_group.buttons()):
                    button.setEnabled(State)
                    self.ui.bases_table.cellWidget(i,2).setEnabled(State)

if __name__ == '__main__':

    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow_()
    win.show()
    #print(win.d_dict)
    #app.exec_()
    sys.exit(app.exec_())


