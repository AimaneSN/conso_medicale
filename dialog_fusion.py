# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'dialog_fusion.ui'
#
# Created by: PyQt5 UI code generator 5.15.7
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.resize(950, 257)
        self.layoutWidget = QtWidgets.QWidget(Dialog)
        self.layoutWidget.setGeometry(QtCore.QRect(13, 3, 931, 251))
        self.layoutWidget.setObjectName("layoutWidget")
        self.gridLayout_2 = QtWidgets.QGridLayout(self.layoutWidget)
        self.gridLayout_2.setContentsMargins(0, 0, 0, 0)
        self.gridLayout_2.setObjectName("gridLayout_2")
        self.verticalLayout_2 = QtWidgets.QVBoxLayout()
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.label = QtWidgets.QLabel(self.layoutWidget)
        self.label.setObjectName("label")
        self.verticalLayout_2.addWidget(self.label)
        self.gridLayout = QtWidgets.QGridLayout()
        self.gridLayout.setObjectName("gridLayout")
        self.label_2 = QtWidgets.QLabel(self.layoutWidget)
        self.label_2.setObjectName("label_2")
        self.gridLayout.addWidget(self.label_2, 0, 0, 1, 1)
        self.label_3 = QtWidgets.QLabel(self.layoutWidget)
        self.label_3.setObjectName("label_3")
        self.gridLayout.addWidget(self.label_3, 1, 0, 1, 1)
        self.label_4 = QtWidgets.QLabel(self.layoutWidget)
        self.label_4.setObjectName("label_4")
        self.gridLayout.addWidget(self.label_4, 0, 2, 1, 1)
        self.combo_base_g = QtWidgets.QComboBox(self.layoutWidget)
        self.combo_base_g.setObjectName("combo_base_g")
        self.gridLayout.addWidget(self.combo_base_g, 0, 1, 1, 1)
        self.label_5 = QtWidgets.QLabel(self.layoutWidget)
        self.label_5.setObjectName("label_5")
        self.gridLayout.addWidget(self.label_5, 1, 2, 1, 1)
        self.combo_base_d = QtWidgets.QComboBox(self.layoutWidget)
        self.combo_base_d.setObjectName("combo_base_d")
        self.gridLayout.addWidget(self.combo_base_d, 1, 1, 1, 1)
        self.combo_cle_g = QtWidgets.QComboBox(self.layoutWidget)
        self.combo_cle_g.setObjectName("combo_cle_g")
        self.gridLayout.addWidget(self.combo_cle_g, 0, 3, 1, 1)
        self.combo_cle_d = QtWidgets.QComboBox(self.layoutWidget)
        self.combo_cle_d.setObjectName("combo_cle_d")
        self.gridLayout.addWidget(self.combo_cle_d, 1, 3, 1, 1)
        self.verticalLayout_2.addLayout(self.gridLayout)
        self.horizontalLayout = QtWidgets.QHBoxLayout()
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.label_6 = QtWidgets.QLabel(self.layoutWidget)
        self.label_6.setObjectName("label_6")
        self.horizontalLayout.addWidget(self.label_6)
        spacerItem = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem)
        self.combo_join_type = QtWidgets.QComboBox(self.layoutWidget)
        self.combo_join_type.setObjectName("combo_join_type")
        self.horizontalLayout.addWidget(self.combo_join_type)
        self.verticalLayout_2.addLayout(self.horizontalLayout)
        self.gridLayout_2.addLayout(self.verticalLayout_2, 0, 0, 1, 1)
        self.verticalLayout = QtWidgets.QVBoxLayout()
        self.verticalLayout.setObjectName("verticalLayout")
        self.label_7 = QtWidgets.QLabel(self.layoutWidget)
        self.label_7.setObjectName("label_7")
        self.verticalLayout.addWidget(self.label_7)
        self.table_variables_a_importer = QtWidgets.QTableWidget(self.layoutWidget)
        self.table_variables_a_importer.setObjectName("table_variables_a_importer")
        self.table_variables_a_importer.setColumnCount(2)
        self.table_variables_a_importer.setRowCount(0)
        item = QtWidgets.QTableWidgetItem()
        self.table_variables_a_importer.setHorizontalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.table_variables_a_importer.setHorizontalHeaderItem(1, item)
        self.verticalLayout.addWidget(self.table_variables_a_importer)
        self.gridLayout_2.addLayout(self.verticalLayout, 0, 1, 2, 1)
        self.horizontalLayout_3 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_3.setObjectName("horizontalLayout_3")
        spacerItem1 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_3.addItem(spacerItem1)
        self.horizontalLayout_2 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_2.setObjectName("horizontalLayout_2")
        self.button_annuler = QtWidgets.QPushButton(self.layoutWidget)
        self.button_annuler.setObjectName("button_annuler")
        self.horizontalLayout_2.addWidget(self.button_annuler)
        self.button_fusionner = QtWidgets.QPushButton(self.layoutWidget)
        self.button_fusionner.setObjectName("button_fusionner")
        self.horizontalLayout_2.addWidget(self.button_fusionner)
        self.horizontalLayout_3.addLayout(self.horizontalLayout_2)
        self.gridLayout_2.addLayout(self.horizontalLayout_3, 2, 1, 1, 1)
        spacerItem2 = QtWidgets.QSpacerItem(17, 15, QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Expanding)
        self.gridLayout_2.addItem(spacerItem2, 1, 0, 1, 1)

        self.retranslateUi(Dialog)
        QtCore.QMetaObject.connectSlotsByName(Dialog)

    def retranslateUi(self, Dialog):
        _translate = QtCore.QCoreApplication.translate
        Dialog.setWindowTitle(_translate("Dialog", "Dialog"))
        self.label.setText(_translate("Dialog", "Veuillez choisir les bases à fusionner :"))
        self.label_2.setText(_translate("Dialog", "Base à gauche :"))
        self.label_3.setText(_translate("Dialog", "Base à droite :"))
        self.label_4.setText(_translate("Dialog", "Clé de la jointure :"))
        self.label_5.setText(_translate("Dialog", "Clé de la jointure :"))
        self.label_6.setText(_translate("Dialog", "Type de la jointure :"))
        self.label_7.setText(_translate("Dialog", "Variables à importer(base à droite) :"))
        item = self.table_variables_a_importer.horizontalHeaderItem(0)
        item.setText(_translate("Dialog", "Inclure"))
        item = self.table_variables_a_importer.horizontalHeaderItem(1)
        item.setText(_translate("Dialog", "Variable"))
        self.button_annuler.setText(_translate("Dialog", "Annuler"))
        self.button_fusionner.setText(_translate("Dialog", "Fusionner"))
