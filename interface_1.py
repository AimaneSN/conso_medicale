# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'interface_1.ui'
#
# Created by: PyQt5 UI code generator 5.15.7
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(978, 723)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.gridLayout = QtWidgets.QGridLayout(self.centralwidget)
        self.gridLayout.setObjectName("gridLayout")
        self.verticalLayout_4 = QtWidgets.QVBoxLayout()
        self.verticalLayout_4.setObjectName("verticalLayout_4")
        self.verticalLayout_3 = QtWidgets.QVBoxLayout()
        self.verticalLayout_3.setObjectName("verticalLayout_3")
        self.horizontalLayout_3 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_3.setObjectName("horizontalLayout_3")
        self.VL_ajouter = QtWidgets.QVBoxLayout()
        self.VL_ajouter.setObjectName("VL_ajouter")
        self.label_ajouter = QtWidgets.QLabel(self.centralwidget)
        self.label_ajouter.setObjectName("label_ajouter")
        self.VL_ajouter.addWidget(self.label_ajouter)
        self.bases_table = QtWidgets.QTableWidget(self.centralwidget)
        self.bases_table.setObjectName("bases_table")
        self.bases_table.setColumnCount(4)
        self.bases_table.setRowCount(0)
        item = QtWidgets.QTableWidgetItem()
        self.bases_table.setHorizontalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.bases_table.setHorizontalHeaderItem(1, item)
        item = QtWidgets.QTableWidgetItem()
        self.bases_table.setHorizontalHeaderItem(2, item)
        item = QtWidgets.QTableWidgetItem()
        self.bases_table.setHorizontalHeaderItem(3, item)
        self.bases_table.horizontalHeader().setDefaultSectionSize(150)
        self.bases_table.verticalHeader().setVisible(False)
        self.bases_table.verticalHeader().setDefaultSectionSize(30)
        self.VL_ajouter.addWidget(self.bases_table)
        self.button_ajouter_base = QtWidgets.QPushButton(self.centralwidget)
        self.button_ajouter_base.setObjectName("button_ajouter_base")
        self.VL_ajouter.addWidget(self.button_ajouter_base)
        self.button_selectionner = QtWidgets.QPushButton(self.centralwidget)
        self.button_selectionner.setObjectName("button_selectionner")
        self.VL_ajouter.addWidget(self.button_selectionner)
        self.button_supprimer = QtWidgets.QPushButton(self.centralwidget)
        self.button_supprimer.setObjectName("button_supprimer")
        self.VL_ajouter.addWidget(self.button_supprimer)
        self.horizontalLayout_3.addLayout(self.VL_ajouter)
        self.verticalLayout_2 = QtWidgets.QVBoxLayout()
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.horizontalLayout_2 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_2.setObjectName("horizontalLayout_2")
        self.label_variables = QtWidgets.QLabel(self.centralwidget)
        self.label_variables.setObjectName("label_variables")
        self.horizontalLayout_2.addWidget(self.label_variables)
        spacerItem = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_2.addItem(spacerItem)
        spacerItem1 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_2.addItem(spacerItem1)
        spacerItem2 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_2.addItem(spacerItem2)
        self.loading_label = QtWidgets.QLabel(self.centralwidget)
        self.loading_label.setText("")
        self.loading_label.setObjectName("loading_label")
        self.horizontalLayout_2.addWidget(self.loading_label)
        spacerItem3 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_2.addItem(spacerItem3)
        spacerItem4 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_2.addItem(spacerItem4)
        self.verticalLayout_2.addLayout(self.horizontalLayout_2)
        self.variables_table = QtWidgets.QTableWidget(self.centralwidget)
        self.variables_table.setObjectName("variables_table")
        self.variables_table.setColumnCount(3)
        self.variables_table.setRowCount(0)
        item = QtWidgets.QTableWidgetItem()
        self.variables_table.setHorizontalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.variables_table.setHorizontalHeaderItem(1, item)
        item = QtWidgets.QTableWidgetItem()
        self.variables_table.setHorizontalHeaderItem(2, item)
        self.variables_table.horizontalHeader().setDefaultSectionSize(200)
        self.verticalLayout_2.addWidget(self.variables_table)
        self.verticalLayout = QtWidgets.QVBoxLayout()
        self.verticalLayout.setObjectName("verticalLayout")
        self.button_enregistrer = QtWidgets.QPushButton(self.centralwidget)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.button_enregistrer.sizePolicy().hasHeightForWidth())
        self.button_enregistrer.setSizePolicy(sizePolicy)
        self.button_enregistrer.setLayoutDirection(QtCore.Qt.LeftToRight)
        self.button_enregistrer.setObjectName("button_enregistrer")
        self.verticalLayout.addWidget(self.button_enregistrer)
        self.button_modifier_variables = QtWidgets.QPushButton(self.centralwidget)
        self.button_modifier_variables.setObjectName("button_modifier_variables")
        self.verticalLayout.addWidget(self.button_modifier_variables)
        self.button_annuler = QtWidgets.QPushButton(self.centralwidget)
        self.button_annuler.setObjectName("button_annuler")
        self.verticalLayout.addWidget(self.button_annuler)
        self.verticalLayout_2.addLayout(self.verticalLayout)
        self.horizontalLayout_3.addLayout(self.verticalLayout_2)
        self.verticalLayout_3.addLayout(self.horizontalLayout_3)
        self.VL_apercu = QtWidgets.QVBoxLayout()
        self.VL_apercu.setObjectName("VL_apercu")
        self.label_apercu = QtWidgets.QLabel(self.centralwidget)
        self.label_apercu.setObjectName("label_apercu")
        self.VL_apercu.addWidget(self.label_apercu)
        self.table_apercu = QtWidgets.QTableView(self.centralwidget)
        self.table_apercu.setObjectName("table_apercu")
        self.VL_apercu.addWidget(self.table_apercu)
        spacerItem5 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.VL_apercu.addItem(spacerItem5)
        self.verticalLayout_3.addLayout(self.VL_apercu)
        self.verticalLayout_4.addLayout(self.verticalLayout_3)
        self.horizontalLayout = QtWidgets.QHBoxLayout()
        self.horizontalLayout.setObjectName("horizontalLayout")
        spacerItem6 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem6)
        self.button_calculs_traitements = QtWidgets.QPushButton(self.centralwidget)
        self.button_calculs_traitements.setObjectName("button_calculs_traitements")
        self.horizontalLayout.addWidget(self.button_calculs_traitements)
        self.button_quitter = QtWidgets.QPushButton(self.centralwidget)
        self.button_quitter.setObjectName("button_quitter")
        self.horizontalLayout.addWidget(self.button_quitter)
        self.verticalLayout_4.addLayout(self.horizontalLayout)
        self.gridLayout.addLayout(self.verticalLayout_4, 0, 0, 1, 1)
        MainWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)
        self.actionAide = QtWidgets.QAction(MainWindow)
        self.actionAide.setObjectName("actionAide")
        self.act_aide = QtWidgets.QAction(MainWindow)
        self.act_aide.setObjectName("act_aide")

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "MainWindow"))
        self.label_ajouter.setText(_translate("MainWindow", "Fichiers ajoutés : "))
        item = self.bases_table.horizontalHeaderItem(0)
        item.setText(_translate("MainWindow", "Fichier"))
        item = self.bases_table.horizontalHeaderItem(1)
        item.setText(_translate("MainWindow", "Etat"))
        item = self.bases_table.horizontalHeaderItem(2)
        item.setText(_translate("MainWindow", "Type de la base"))
        item = self.bases_table.horizontalHeaderItem(3)
        item.setText(_translate("MainWindow", "Année d\'inventaire"))
        self.button_ajouter_base.setText(_translate("MainWindow", "Ajouter une base"))
        self.button_selectionner.setText(_translate("MainWindow", "Sélectionner"))
        self.button_supprimer.setText(_translate("MainWindow", "Supprimer"))
        self.label_variables.setText(_translate("MainWindow", "Variables du fichier sélectionné :"))
        item = self.variables_table.horizontalHeaderItem(0)
        item.setText(_translate("MainWindow", "Colonne"))
        item = self.variables_table.horizontalHeaderItem(1)
        item.setText(_translate("MainWindow", "Variable"))
        item = self.variables_table.horizontalHeaderItem(2)
        item.setText(_translate("MainWindow", "Format (dates)"))
        self.button_enregistrer.setText(_translate("MainWindow", "Enregistrer"))
        self.button_modifier_variables.setText(_translate("MainWindow", "Modifier les variables"))
        self.button_annuler.setText(_translate("MainWindow", "Annuler"))
        self.label_apercu.setText(_translate("MainWindow", "Apercu de la base selectionnee :"))
        self.button_calculs_traitements.setText(_translate("MainWindow", "Traitements/calcul des inputs"))
        self.button_quitter.setText(_translate("MainWindow", "Quitter"))
        self.actionAide.setText(_translate("MainWindow", "Aide"))
        self.act_aide.setText(_translate("MainWindow", "Aide"))
