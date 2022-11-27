

from PyQt5 import QtCore, QtGui, QtWidgets
from dialog_modifier import Ui_Dialog

import coldict

LABEL_BASE_ASSIETTE = "DEMOGRAPHIQUE"
LABEL_BASE_CONSO = "CONSOMMATIONS"
LABEL_BASE_ALD =  "ALD/ALC"
LABEL_BASE_MEDIC = "MEDICAMENTS"

class dialog_modifier(QtWidgets.QDialog):

    def __init__(self, label_base, bttn_group, table, parent = None):
        super(dialog_modifier, self).__init__(parent)

        self.setModal(True)
        self.uid = Ui_Dialog()
        self.uid.setupUi(self)        
        
        chkId = bttn_group.checkedId()

        self.label_base = table.cellWidget(chkId, 2).currentText()
        self.dict_indices = {}
        
        if (self.label_base == LABEL_BASE_ASSIETTE):
            self.dict_etat = coldict.assiette_etat #Si dict_etat est modifié, assiette_etat est également modifié.

        elif (self.label_base == LABEL_BASE_CONSO):
            self.dict_etat = coldict.conso_etat

        elif (self.label_base == LABEL_BASE_ALD):
            self.dict_etat = coldict.ald_etat

        elif (self.label_base == LABEL_BASE_MEDIC):
            self.dict_etat = coldict.medic_etat
        
        self.cols = list(self.dict_etat)

        for col in self.cols:
            rowPos = self.uid.table_variables_dialog.rowCount()
            chk_box = QtWidgets.QCheckBox()

            if self.dict_etat[col] == True:
                chk_box.setChecked(True)
            else:
                chk_box.setChecked(False)

            self.uid.table_variables_dialog.insertRow(rowPos)
            self.uid.table_variables_dialog.setCellWidget(rowPos, 0, chk_box)
            self.uid.table_variables_dialog.setItem(rowPos, 1, QtWidgets.QTableWidgetItem(col))        
        
        self.uid.button_quitter_dialog.clicked.connect(self.reject)
        self.uid.button_enregistrer_dialog.clicked.connect(self.accept)

    def accept(self):

        nrow = self.uid.table_variables_dialog.rowCount()
        for i in range(0, nrow):

            chk = self.uid.table_variables_dialog.cellWidget(i, 0)
            if chk.isChecked():
                self.dict_etat[self.cols[i]] = True
                continue
            self.dict_etat[self.cols[i]] = False

        super().accept()
        
    def get_dict(self):
        return self.dict_indices
