# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'modalites_dialog.ui'
#
# Created by: PyQt5 UI code generator 5.15.7
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.resize(635, 399)
        self.layoutWidget = QtWidgets.QWidget(Dialog)
        self.layoutWidget.setGeometry(QtCore.QRect(3, 3, 621, 391))
        self.layoutWidget.setObjectName("layoutWidget")
        self.gridLayout = QtWidgets.QGridLayout(self.layoutWidget)
        self.gridLayout.setContentsMargins(0, 0, 0, 0)
        self.gridLayout.setObjectName("gridLayout")
        self.verticalLayout_3 = QtWidgets.QVBoxLayout()
        self.verticalLayout_3.setObjectName("verticalLayout_3")
        self.verticalLayout = QtWidgets.QVBoxLayout()
        self.verticalLayout.setObjectName("verticalLayout")
        self.modalites_label = QtWidgets.QLabel(self.layoutWidget)
        self.modalites_label.setObjectName("modalites_label")
        self.verticalLayout.addWidget(self.modalites_label)
        self.horizontalLayout = QtWidgets.QHBoxLayout()
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.label_2 = QtWidgets.QLabel(self.layoutWidget)
        self.label_2.setObjectName("label_2")
        self.horizontalLayout.addWidget(self.label_2)
        self.combo_variables = QtWidgets.QComboBox(self.layoutWidget)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.combo_variables.sizePolicy().hasHeightForWidth())
        self.combo_variables.setSizePolicy(sizePolicy)
        self.combo_variables.setObjectName("combo_variables")
        self.horizontalLayout.addWidget(self.combo_variables)
        self.button_select_var = QtWidgets.QPushButton(self.layoutWidget)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.button_select_var.sizePolicy().hasHeightForWidth())
        self.button_select_var.setSizePolicy(sizePolicy)
        self.button_select_var.setObjectName("button_select_var")
        self.horizontalLayout.addWidget(self.button_select_var)
        spacerItem = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem)
        spacerItem1 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem1)
        self.verticalLayout.addLayout(self.horizontalLayout)
        self.verticalLayout_3.addLayout(self.verticalLayout)
        self.verticalLayout_2 = QtWidgets.QVBoxLayout()
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.horizontalLayout_4 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_4.setObjectName("horizontalLayout_4")
        spacerItem2 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_4.addItem(spacerItem2)
        self.table_modalites = QtWidgets.QTableWidget(self.layoutWidget)
        self.table_modalites.setObjectName("table_modalites")
        self.table_modalites.setColumnCount(2)
        self.table_modalites.setRowCount(0)
        item = QtWidgets.QTableWidgetItem()
        self.table_modalites.setHorizontalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.table_modalites.setHorizontalHeaderItem(1, item)
        self.horizontalLayout_4.addWidget(self.table_modalites)
        spacerItem3 = QtWidgets.QSpacerItem(58, 13, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_4.addItem(spacerItem3)
        self.verticalLayout_2.addLayout(self.horizontalLayout_4)
        self.horizontalLayout_3 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_3.setObjectName("horizontalLayout_3")
        spacerItem4 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_3.addItem(spacerItem4)
        self.button_enregistrer_var = QtWidgets.QPushButton(self.layoutWidget)
        self.button_enregistrer_var.setObjectName("button_enregistrer_var")
        self.horizontalLayout_3.addWidget(self.button_enregistrer_var)
        self.button_annuler_var = QtWidgets.QPushButton(self.layoutWidget)
        self.button_annuler_var.setObjectName("button_annuler_var")
        self.horizontalLayout_3.addWidget(self.button_annuler_var)
        spacerItem5 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_3.addItem(spacerItem5)
        self.verticalLayout_2.addLayout(self.horizontalLayout_3)
        self.verticalLayout_3.addLayout(self.verticalLayout_2)
        self.gridLayout.addLayout(self.verticalLayout_3, 0, 0, 1, 2)
        spacerItem6 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.gridLayout.addItem(spacerItem6, 1, 0, 1, 1)
        self.horizontalLayout_2 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_2.setObjectName("horizontalLayout_2")
        spacerItem7 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_2.addItem(spacerItem7)
        self.button_terminer_mod = QtWidgets.QPushButton(self.layoutWidget)
        self.button_terminer_mod.setObjectName("button_terminer_mod")
        self.horizontalLayout_2.addWidget(self.button_terminer_mod)
        self.button_quitter_mod = QtWidgets.QPushButton(self.layoutWidget)
        self.button_quitter_mod.setObjectName("button_quitter_mod")
        self.horizontalLayout_2.addWidget(self.button_quitter_mod)
        self.gridLayout.addLayout(self.horizontalLayout_2, 1, 1, 1, 1)

        self.retranslateUi(Dialog)
        QtCore.QMetaObject.connectSlotsByName(Dialog)

    def retranslateUi(self, Dialog):
        _translate = QtCore.QCoreApplication.translate
        Dialog.setWindowTitle(_translate("Dialog", "Dialog"))
        self.modalites_label.setText(_translate("Dialog", "Veuillez choisir les modalités pour chaque variable."))
        self.label_2.setText(_translate("Dialog", "Variable à traiter : "))
        self.button_select_var.setText(_translate("Dialog", "Sélectionner"))
        item = self.table_modalites.horizontalHeaderItem(0)
        item.setText(_translate("Dialog", "Modalites"))
        item = self.table_modalites.horizontalHeaderItem(1)
        item.setText(_translate("Dialog", "Choix"))
        self.button_enregistrer_var.setText(_translate("Dialog", "Enregistrer la variable"))
        self.button_annuler_var.setText(_translate("Dialog", "Annuler"))
        self.button_terminer_mod.setText(_translate("Dialog", "Terminer "))
        self.button_quitter_mod.setText(_translate("Dialog", "Quitter"))
