
from PyQt5 import QtCore, QtGui, QtWidgets
from dialog_traitement import Ui_Dialog

import findspark
findspark.init()
import pyspark
import os

from pyspark.sql import SparkSession, Row, HiveContext, Window
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import  pyspark.sql.functions  as F

import numpy as np
import pandas as pd

import colnames
import coldict
import readfile
import utils
import operations_inputs
import calculs
from datetime import datetime

LABEL_BASE_ASSIETTE = "DEMOGRAPHIQUE"
LABEL_BASE_CONSO = "CONSOMMATIONS"
LABEL_BASE_ALD =  "ALD/ALC"
LABEL_BASE_MEDIC = "MEDICAMENTS"

from script_dialog_fusionner import dialog_fusionner
from script_dialog_freq_conso import dialog_freqconso

class MyTableModel(QtCore.QAbstractTableModel):   
    #Permet l'aperçu des inputs

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

class dialog_traitements(QtWidgets.QDialog):

    def __init__(self, dict_indexes, dict_types, dict_paths, dict_mod, dict_all_mods, parent = None):
        super(dialog_traitements, self).__init__(parent)
        self.ses_sp = SparkSession.builder.config("spark.driver.memory", "2g").appName('mods').enableHiveSupport().getOrCreate()

        self.setModal(True)
        self.uit = Ui_Dialog()
        self.uit.setupUi(self)      
        
        self.dict_indexes = dict_indexes
        self.dict_types = dict_types
        self.dict_paths = dict_paths
        self.dict_df = {}
        self.nb_manquants = []

        self.dict_mod = dict_mod
        self.dict_all_mods = dict_all_mods

        self.l_bases = list(self.dict_indexes) #liste des libellés des bases importées
   
        self.df_vars = []
        
        for base in self.l_bases: #base : tuple

            #Importation des bases ajoutées, renommage et conversion des variables. Calcul de l'âge, de l'âge quinquennal, ... Traitement des modalités aberrantes.
            current_path = self.dict_paths[base]
            sep = readfile.get_colnames(current_path)[-1]
            
            if base in self.dict_mod.keys():
                self.dict_df[base] = self.dict_mod[base]
            else:
                self.dict_df[base] = readfile.get_file(current_path, sep, self.ses_sp)
            
            self.dict_df[base] = utils.rename_columns(self.dict_df[base], self.dict_indexes[base])
            self.dict_df[base] = utils.convert_columns(self.dict_df[base], self.dict_types[base[0]])

            if base[0] == LABEL_BASE_ASSIETTE:
                self.dict_df[base] = operations_inputs.input_variables_demo(self.dict_df[base], base[1]) #base[1] : année d'inventaire saisie par l'utilisateur
                self.dict_df[base] = calculs.age_operations(self.dict_df[base], base[1], 0) #Traitement manquants+aberrants
            
            elif base[0] == LABEL_BASE_ALD:
                self.dict_df[base] = calculs.operations_ald(self.dict_df[base], base[1])
            
            elif base[0] == LABEL_BASE_CONSO:
                self.dict_df[base], dict_tardifs = calculs.conso_select(self.dict_df[base], base[1])
                self.dict_df[base] = calculs.calculs_conso(self.dict_df[base])
            
            #Traitement des modalités aberrantes (i.e. celles qui ne sont attribuées à aucune modalité prédéfinie)
            for var in self.dict_df[base].columns:
                if var in self.dict_all_mods.keys():
                    #print(self.dict_df[base].select(var).distinct().rdd.flatMap(lambda x: x).collect())
                    mod_list_all = list(self.dict_all_mods[var])
                    mods = {k : v for k, v in self.dict_all_mods[var].items() if v != None}
                    if mods != self.dict_all_mods[var]: # Il y a au moins une modalité non attribuée
                        self.dict_df[base] = calculs.manquants_mod_conso(self.dict_df[base], var, mods, mod_list_all)[0]
                
            L_schema = [ele.simpleString().split(":") for ele in self.dict_df[base].schema]
            dict_schema = {ele[0] : ele[1] for ele in L_schema} #{"nom_colonne" : "type_colonne"}
            L_vars = list(dict_schema.keys()) #Liste des variables de la base en cours
            
            #Remplissage de la table "tree_vars_stats" avec les noms des bases et des variables importées
            base_item = QtWidgets.QTreeWidgetItem(self.uit.tree_vars_stats)
            base_item.setText(0, base[0])
            base_item.setText(1, str(base[1]))
            
            for var in L_vars:
                var_item = QtWidgets.QTreeWidgetItem(base_item)

                var_item.setText(0, var) #Nom de la variable
                var_item.setText(2, dict_schema[var]) #Type de la variable
            
            self.df_vars.extend(self.dict_df[base].columns)
        
        self.vars_traites = dict.fromkeys(self.df_vars, False) #Dictionnaire des états des variables, False: non traitée, True : traitée.
        
        #Remplissage de la table "tree_inputs"
        for table_input in coldict.dict_inputs.keys():
            t_item = QtWidgets.QTreeWidgetItem(self.uit.tree_inputs)
            t_item.setText(0, table_input)

            for nom_input in coldict.dict_inputs[table_input].keys():
                input_item = QtWidgets.QTreeWidgetItem(t_item)
                input_item.setText(0, nom_input)
            
        self.uit.button_lancer_stat.clicked.connect(self.lancer_stat)
        self.uit.button_lancer_stat_post.clicked.connect(self.lancer_stat_post)
        self.uit.button_selectionner_input.clicked.connect(self.selectionner_input)
        self.uit.button_enregistrer_input.clicked.connect(self.enregistrer_input)
        self.uit.button_exporter.clicked.connect(self.exporter_input)
        self.uit.button_fusionner.clicked.connect(self.lancer_fusion)
        self.uit.button_selectionner_var.clicked.connect(self.traitement_var)
        self.uit.button_annuler_input.clicked.connect(self.annuler_input)
        self.uit.button_lancer_traitement.clicked.connect(self.lancer_traitement)

        self.uit.button_fusionner.setEnabled(False)
        self.uit.button_enregistrer_input.setEnabled(False)
        self.uit.button_exporter.setEnabled(False)
        self.uit.button_annuler_input.setEnabled(False)
        self.uit.button_annuler_apercu.setEnabled(False)
        
    def lancer_stat(self):
        
        self.current_var = self.uit.tree_vars_stats.currentItem()
        if (not self.current_var) or (self.current_var.text(0) in self.l_bases):
            return
        
        self.current_parent = self.current_var.parent().text(0)
        self.current_year = int(self.current_var.parent().text(1))

        #Observés:
        N_obs = self.dict_df[(self.current_parent, self.current_year)].filter(F.col(self.current_var.text(0)).isNotNull()).count()
        #Manquants:
        N_null = self.dict_df[(self.current_parent, self.current_year)].filter(F.col(self.current_var.text(0)).isNull()).count()
        #Total:
        N_tot = N_obs + N_null

        #Proportions observés/manquants:
        N_obs_prop = round((N_obs/N_tot) * 100, 3)
        N_null_prop = round((N_null/N_tot) * 100, 3)

        #Remplissage de la table avec les effectifs ci-dessus:
        self.current_var.setText(3, str(N_obs) + " (" + str(N_obs_prop)+ "%)")
        self.current_var.setText(4, str(N_null) + " (" + str(N_null_prop)+ "%)")

        #Statistiques descriptives si la variable est un float ou int:
        if self.current_var.text(2) in ["int", "bigint", "double"]:
            stats = self.dict_df[(self.current_parent, self.current_year)].agg(F.mean(F.col(self.current_var.text(0))),
                                                    F.sum(F.col(self.current_var.text(0))),
                                                    F.min(F.col(self.current_var.text(0))),
                                                    F.max(F.col(self.current_var.text(0)))).rdd.flatMap(lambda x: x).collect()

            self.current_var.setText(5, str(round(stats[0], 3)))
            self.current_var.setText(6, str(round(stats[1], 3)))
            self.current_var.setText(7, str(round(stats[2], 3)))
            self.current_var.setText(8, str(round(stats[3], 3)))

        else:
            self.current_var.setText(5, "NN")
            self.current_var.setText(6, "NN")
            self.current_var.setText(7, "NN")
            self.current_var.setText(8, "NN")

    def lancer_stat_post(self):

        self.current_var_p = self.uit.tree_vars_stats_post.currentItem()
        if (not self.current_var_p) or (self.current_var_p.text(0) in self.l_bases):
            return
        
        self.current_parent_p = self.current_var_p.parent().text(0)
        self.current_year_p = int(self.current_var_p.parent().text(1))

        #Observés:
        N_obs = self.dict_df[(self.current_parent_p, self.current_year_p)].filter(F.col(self.current_var_p.text(0)).isNotNull()).count()
        #Manquants:
        N_null = self.dict_df[(self.current_parent_p, self.current_year_p)].filter(F.col(self.current_var_p.text(0)).isNull()).count()
        #Total:
        N_tot = N_obs + N_null

        #Proportions observés/manquants:
        N_obs_prop = round((N_obs/N_tot) * 100, 3)
        N_null_prop = round((N_null/N_tot) * 100, 3)

        #Remplissage de la table avec les effectifs ci-dessus:
        self.current_var_p.setText(3, str(N_obs) + " (" + str(N_obs_prop)+ "%)")
        self.current_var_p.setText(4, str(N_null) + " (" + str(N_null_prop)+ "%)")

        #Statistiques descriptives si la variable est un float ou int:
        if self.current_var_p.text(2) in ["int", "bigint", "double"]:
            stats = self.dict_df[(self.current_parent_p, self.current_year_p)].agg(F.mean(F.col(self.current_var_p.text(0))),
                                                    F.sum(F.col(self.current_var_p.text(0))),
                                                    F.min(F.col(self.current_var_p.text(0))),
                                                    F.max(F.col(self.current_var_p.text(0)))).rdd.flatMap(lambda x: x).collect()

            self.current_var_p.setText(5, str(round(stats[0], 3)))
            self.current_var_p.setText(6, str(round(stats[1], 3)))
            self.current_var_p.setText(7, str(round(stats[2], 3)))
            self.current_var_p.setText(8, str(round(stats[3], 3)))

        else:
            self.current_var_p.setText(5, "NN")
            self.current_var_p.setText(6, "NN")
            self.current_var_p.setText(7, "NN")
            self.current_var_p.setText(8, "NN")
    
    def selectionner_input(self):
        self.current_input = self.uit.tree_inputs.currentItem()
        if self.current_input:
            self.current_input = self.current_input.text(0)
        else:
            return
        
        #Définition des variables requises et des paramètres pour chaque input
        self.var_requises = []
        self.params = {}

        if self.current_input == coldict.STR_input_effectifs_salaires_actifs:
            self.vars_requises = [colnames.TYPE_ADHERENT, colnames.TYPE_BENEFICIAIRE, colnames.SEXE, colnames.ASSIETTE_COTISATION, colnames.DNAISSANCE]
            self.params = {}
            self.calcul_input = operations_inputs.input_effectifs_salaires_actifs
        
        elif self.current_input == coldict.STR_input_nvx_actifs:
            self.vars_requises = [colnames.TYPE_ADHERENT, colnames.TYPE_BENEFICIAIRE, colnames.DATE_IMMATRICULATION, colnames.SEXE, colnames.ASSIETTE_COTISATION, colnames.DNAISSANCE] 
            self.params = dict.fromkeys(["annee", "age_min", "age_max"])
            self.calcul_input = operations_inputs.input_nvx_actifs
        
        elif self.current_input == coldict.STR_input_effectifs_pensions_retraites:
            self.vars_requises = [colnames.TYPE_ADHERENT, colnames.TYPE_BENEFICIAIRE, colnames.SEXE, colnames.DNAISSANCE, colnames.ASSIETTE_COTISATION]
            self.params = {}
            self.calcul_input = operations_inputs.input_effectifs_pensions_retraites
        
        elif self.current_input == coldict.STR_input_effectifs_pensions_conjoints_surv:
            self.vars_requises = [colnames.TYPE_ADHERENT, colnames.TYPE_BENEFICIAIRE, colnames.SEXE, colnames.DNAISSANCE, colnames.ASSIETTE_COTISATION]
            self.params = {}
            self.calcul_input = operations_inputs.input_effectifs_pensions_conjoints_surv
        
        elif self.current_input == coldict.STR_taux_masc:
            self.vars_requises = [colnames.TYPE_BENEFICIAIRE, colnames.DNAISSANCE, colnames.SEXE]
            self.params = {}
            self.calcul_input = operations_inputs.input_taux_masc
        
        elif self.current_input == coldict.STR_densite_travail:
            self.vars_requises = [colnames.TYPE_ADHERENT, colnames.TYPE_BENEFICIAIRE, colnames.DATE_IMMATRICULATION, colnames.DNAISSANCE, colnames.SEXE]
            self.params = dict.fromkeys(["annee"])
            self.calcul_input = operations_inputs.input_densite_travail

        elif self.current_input == coldict.STR_taux_prevalence_ALD:
            self.vars_requises = [colnames.DNAISSANCE, colnames.SEXE, "is_ALD"]
            self.calcul_input = operations_inputs.input_taux_prevalence_ALD
        
        elif self.current_input in [coldict.STR_distrib_salaires_pensions_moy_actifs, coldict.STR_distrib_salaires_pensions_moy_pensionnes]:
            self.vars_requises = [colnames.DNAISSANCE, colnames.SEXE, colnames.TYPE_ADHERENT, colnames.TYPE_BENEFICIAIRE, colnames.ASSIETTE_COTISATION]
            self.calcul_input = operations_inputs.input_distrib_assiettes
        
        elif self.current_input == coldict.STR_freq_conso:
            self.vars_requises = [colnames.DNAISSANCE, "age_q", colnames.SEXE, "is_ALD", colnames.TAUX_REMBOURSEMENT, colnames.PRIX_UNITAIRE, colnames.CODE_ACTE]
        
        elif self.current_input == coldict.STR_freq_conso_medic:
            self.vars_requises = [colnames.DNAISSANCE, colnames.SEXE, "is_ALD", "age_q", colnames.TAUX_REMBOURSEMENT_M, colnames.PRIX_UNITAIRE_M, colnames.CODE_MEDIC]
        
        self.l_params = list(self.params)

        #Remplissage des tables "table_variables_requises" et "table_params"
        self.uit.table_variables_requises.setRowCount(0)
        self.uit.table_params.setRowCount(0)

        for var in self.vars_requises:
            rowPos = self.uit.table_variables_requises.rowCount()
            self.uit.table_variables_requises.insertRow(rowPos)
            self.uit.table_variables_requises.setItem(rowPos, 0, QtWidgets.QTableWidgetItem(var))

            #Disponibilté de la variable
            if var not in self.df_vars: #Si une variable requise est introuvable
                self.uit.table_variables_requises.setItem(rowPos, 1, QtWidgets.QTableWidgetItem("Non disponible"))
                self.uit.table_variables_requises.setItem(rowPos, 2, QtWidgets.QTableWidgetItem("NA"))
                continue

            self.uit.table_variables_requises.setItem(rowPos, 1, QtWidgets.QTableWidgetItem("Disponible"))

            #Source de la variable
            for base in self.dict_df.keys():
                if var in self.dict_df[base].columns:
                    if isinstance(base, tuple):
                        self.uit.table_variables_requises.setItem(rowPos, 2, QtWidgets.QTableWidgetItem(str(base[0])+", "+str(base[1])))
                    elif isinstance(base, str):
                        self.uit.table_variables_requises.setItem(rowPos, 2, QtWidgets.QTableWidgetItem(base))

        self.sources = [self.uit.table_variables_requises.item(row, 2).text() for row in range(0, self.uit.table_variables_requises.rowCount())]
        
        if "NA" not in self.sources:
            if len(set(self.sources)) > 1:
                self.uit.button_fusionner.setEnabled(True)
            elif len(set(self.sources)) == 1:
                self.uit.button_enregistrer_input.setEnabled(True)
                self.uit.button_fusionner.setEnabled(False)

        if self.params != {}:
            for param in self.params.keys():
                rowPos = self.uit.table_params.rowCount()
                self.uit.table_params.insertRow(rowPos)
                self.uit.table_params.setItem(rowPos, 0, QtWidgets.QTableWidgetItem(param))

                param_editor = QtWidgets.QLineEdit()
                self.uit.table_params.setCellWidget(rowPos, 1, param_editor)

                if param in ["annee", "age_min", "age_max"]:
                    int_validator = QtGui.QIntValidator()
                    param_editor.setValidator(int_validator)  
        
        #Vidage de l'apercu des inputs
        empty_model = MyTableModel([])
        self.uit.apercu_input.setModel(empty_model)

        self.uit.button_annuler_input.setEnabled(True)
        self.uit.button_selectionner_input.setEnabled(False)

    def enregistrer_input(self):

        #Récupération des paramètres
        if self.params != {}:
            for i, param in enumerate(self.l_params):
                bool_param = True
                param_QLineEdit = self.uit.table_params.cellWidget(i,1)

                if param_QLineEdit.text() == "":
                    pos = param_QLineEdit.cursorRect().bottomLeft()
                    param_QLineEdit.setStyleSheet('border: 3px solid red')
                    QtWidgets.QToolTip.showText(param_QLineEdit.mapToGlobal(pos), "Veuillez saisir le parametre")
                    bool_param = False
                else:
                    param_QLineEdit.setStyleSheet('')
    
                self.params[param] = param_QLineEdit.text()

            if not bool_param: #Un paramètre n'a pas été saisi
                return
        
        if ((colnames.DNAISSANCE in self.vars_requises) or ("age" in self.vars_requises)) and (self.vars_traites["age"] == False) :
            self.showdialog("La variable age n'a pas ete traitee, veuillez faire les traitements dans l'onglet \"Analyse exploratoire/Traitements\" puis reessayer.", "Erreur")
            return
            
        #Obtention de la base source
        source = list(set(self.sources))[0]

        if "JOINTURE" not in source: #La source n'est pas une base fusionnée
            source = (source.split(', ')[0], int(source.split(', ')[1]))
        
        base = self.dict_df[source]
        
        #Calcul des inputs
        if self.current_input == coldict.STR_taux_prevalence_ALD:
            self.df_input = self.calcul_input(base, self.nb_manquants)
        
        elif self.current_input == coldict.STR_distrib_salaires_pensions_moy_actifs:
            self.df_input = self.calcul_input(base)[0]
        elif self.current_input == coldict.STR_distrib_salaires_pensions_moy_pensionnes:
            self.df_input = self.calcul_input(base)[1]
        
        elif self.current_input == coldict.STR_freq_conso:
            dialog_f = dialog_freqconso(base)
            freqconso_value = dialog_f.exec()

            if (freqconso_value == dialog_f.Accepted):
                actes_ph = dialog_f.get_actes_pharma()
                choix = dialog_f.get_choix_matrice()

                if choix == coldict.MATRICE_CONSO:
                    self.df_input, pre, post = operations_inputs.matrice_conso(base, actes_ph)
                    dict_eff = operations_inputs.effectifs_cat_demo(base)
                    self.df_input = operations_inputs.freq_conso(self.df_input, dict_eff)
                
                elif choix == coldict.MATRICE_PHARMA:
                    self.df_input, pre, post = operations_inputs.matrice_conso_ph(base, actes_ph)
                    dict_eff = operations_inputs.effectifs_cat_demo(base)
                    self.df_input = operations_inputs.freq_conso(self.df_input, dict_eff)
                
            else:
                self.showdialog("Vous n'avez pas choisi la matrice de consommation a calculer. Impossible de continuer.", "Erreur")
                return
        
        elif self.current_input == coldict.STR_freq_conso_medic:
            self.df_input, pre, post = operations_inputs.matrice_conso_medic(base)
            dict_eff = operations_inputs.effectifs_cat_demo(base)
            self.df_input = operations_inputs.freq_conso(self.df_input, dict_eff)

        else:
            self.df_input = self.calcul_input(base, **self.params)

        c = self.df_input.count()

        if c > 0 :
            data_ = self.df_input.rdd.flatMap(lambda x: x).collect()
            data = np.array_split(data_, c)
            data = [self.df_input.columns] + [list(row) for row in data]

            #Remplissage de l'aperçu
            model=MyTableModel(data)
            self.uit.apercu_input.setModel(model)
            self.uit.apercu_input.resizeColumnsToContents()

            self.uit.button_annuler_apercu.setEnabled(True)
            self.uit.button_exporter.setEnabled(True)
        
        else: #Le résultat des calculs est un dataframe vide
            self.showdialog("Input vide, veuillez verifier que vous avez bien choisi les modalites, puis reessayer", "Erreur")
            return
    
    def lancer_fusion(self):
        
        dialog_f = dialog_fusionner(self.dict_df, self.sources)

        fusion_value = dialog_f.exec()

        if (fusion_value == dialog_f.Accepted):
            df_value = {str(k) : v for k, v in dialog_f.tuple_df.items()}

            self.dict_df = df_value
            self.nb_manquants = dialog_f.get_missing()
        
        self.selectionner_input()

    def traitement_var(self):

        self.uit.table_manquants.setRowCount(0)
        self.uit.table_aberrants.setRowCount(0)

        self.current_var = self.uit.tree_vars_stats.currentItem()

        if (not self.current_var) or (self.current_var.text(0) in self.l_bases):
            return
        
        self.current_parent = self.current_var.parent().text(0)
        self.current_year = int(self.current_var.parent().text(1))

        current_path = self.dict_paths[(self.current_parent, self.current_year)]
        sep = readfile.get_colnames(current_path)[-1]
        
        l_categories = [colnames.TYPE_ADHERENT, colnames.TYPE_BENEFICIAIRE, colnames.SEXE]
        parent_vars = list(self.dict_indexes[(self.current_parent, self.current_year)].keys())
        self.varname = self.current_var.text(0)
       
        categories_disp = list(set(parent_vars).intersection(l_categories))

        #Obtention de l'effectif des observations manquantes par catégorie démographique
        if categories_disp != []:
            df_manquants = self.dict_df[(self.current_parent, self.current_year)].filter(F.col(self.varname).isNull()) \
                                                            .groupBy(categories_disp).count() \
                                                            .sort(categories_disp)
        else:
            df_manquants = self.dict_df[(self.current_parent, self.current_year)].filter(F.col(self.varname).isNull())
        
        c = df_manquants.count()
        
        if c == 0:
            self.uit.table_manquants.insertRow(0)
            self.uit.table_manquants.setItem(0, 0, QtWidgets.QTableWidgetItem("Toutes"))
            self.uit.table_manquants.setItem(0, 1, QtWidgets.QTableWidgetItem("0"))
        
        else:
            data_ = df_manquants.rdd.flatMap(lambda x: x).collect()
            data = np.array_split(data_, c)
            data = [df_manquants.columns] + [list(row) for row in data]  
            data = [ [', '.join(row[:-1]), row[-1]] for row in data[1:]]
            
            #Remplissage de la table des manquants par catégorie démographique
            N = len(data)
            for i in range(N):
                crow = data[i]
                rowPos = self.uit.table_manquants.rowCount()
                self.uit.table_manquants.insertRow(rowPos)
                self.uit.table_manquants.setItem(rowPos, 0, QtWidgets.QTableWidgetItem(crow[0]))
                self.uit.table_manquants.setItem(rowPos, 1, QtWidgets.QTableWidgetItem(crow[1]))
        
        #Aberrations
        if self.varname == "age":
            dict_aberr = calculs.age_aberr(self.dict_df[(self.current_parent, self.current_year)])

        elif self.varname == colnames.ASSIETTE_COTISATION:
            dict_aberr = calculs.assiette_aberr(self.dict_df[(self.current_parent, self.current_year)])

        elif self.varname == colnames.SEXE:
            dict_aberr = calculs.sexe_aberr(self.dict_df[(self.current_parent, self.current_year)])

        elif self.varname == colnames.MONTANT_ENGAGE:
            dict_aberr = {"Montant engage negatif (<0):" : self.dict_df[(self.current_parent, self.current_year)].filter(F.col(self.varname) < 0).count()}

        elif self.varname in [colnames.TYPE_ACTE, colnames.SECTEUR_SOIN]:
            pass

        for aberr, eff in dict_aberr.items():
            rowPos = self.uit.table_aberrants.rowCount()
            self.uit.table_aberrants.insertRow(rowPos)
            self.uit.table_aberrants.setItem(rowPos, 0, QtWidgets.QTableWidgetItem(aberr))
            self.uit.table_aberrants.setItem(rowPos, 1, QtWidgets.QTableWidgetItem(str(eff)))
    
    def lancer_traitement(self):

        if self.vars_traites[self.varname] == True:
            confirm = self.showdialog("La variable"+ self.varname+ "a deja ete traitee. Etes-vous surs de vouloir continuer ?", "Confirmation")
            if not confirm:
                return
        
        if self.varname == "age":
            self.dict_df[(self.current_parent, self.current_year)] = self.dict_df[(self.current_parent, self.current_year)].withColumn("age", F.col("age_corr"))
        elif self.varname == colnames.ASSIETTE_COTISATION:
            self.dict_df[(self.current_parent, self.current_year)] = calculs.assiette_operations(self.dict_df[(self.current_parent, self.current_year)])
        elif self.varname == colnames.SEXE:
            self.dict_df[(self.current_parent, self.current_year)] = calculs.sexe_operations(self.dict_df[(self.current_parent, self.current_year)])
        
        #Vérification que la variable/la base n'existe pas déjà dans la table "tree_vars_stats_post"
        L_schema = [ele.simpleString().split(":") for ele in self.dict_df[(self.current_parent, self.current_year)].schema]
        dict_schema = {ele[0] : ele[1] for ele in L_schema} #{"nom_colonne" : "type_colonne"}
        
        iterator = QtWidgets.QTreeWidgetItemIterator(self.uit.tree_vars_stats_post)
        foundchild = 0
        parent_obj = None
        while iterator.value():
            item = iterator.value()
            if item.text(0) == self.current_parent:
                parent_obj = item
            
            if parent_obj != None and item.text(0) == self.varname:
                foundchild = 1
            
            iterator+=1

        if parent_obj == None:
            base_item = QtWidgets.QTreeWidgetItem(self.uit.tree_vars_stats_post)
            base_item.setText(0, self.current_parent)
            base_item.setText(1, str(self.current_year))

            var_item = QtWidgets.QTreeWidgetItem(base_item)

            var_item.setText(0, self.varname) #Nom de la variable
            var_item.setText(2, dict_schema[self.varname]) #Type de la variable

        elif parent_obj != None and foundchild == 0:
            var_item = QtWidgets.QTreeWidgetItem(parent_obj)
            var_item.setText(0, self.varname) 
            var_item.setText(2, dict_schema[self.varname])
        
        self.showdialog("La variable "+ self.varname+ " a ete traitee avec succes.", "Succes")
        self.vars_traites[self.varname] = True
        
    def exporter_input(self):
        
        fname = QtWidgets.QFileDialog.getSaveFileName(self.uit.tabWidget, "Exportation de l'input", "Input", "Fichier texte (*.txt);;Fichier Excel (*.xlsx);;Fichier CSV(*.csv)")
        if fname[0] == '':
            return
        
        pd_output = self.df_input.toPandas()
        cols = list(pd_output.columns)
        if self.current_input in coldict.dict_t_pop_init.keys() :
            output = np.zeros((6, 111), dtype = float)
            for _ , row in pd_output.iterrows():
                if row[colnames.SEXE] == coldict.sexe_mod[coldict.SEXE_M]:
                    output[0, int(row["age"])] = int(row[cols[2]])
                    output[2, int(row["age"])] = float(row[cols[3]])

                elif row[colnames.SEXE] == coldict.sexe_mod[coldict.SEXE_F]:
                    output[1, int(row["age"])] = int(row[cols[2]])
                    output[3, int(row["age"])] = float(row[cols[3]])

            if self.current_input == coldict.STR_input_nvx_actifs:
                for _ , row in pd_output.iterrows():
                    if row[colnames.SEXE] == coldict.sexe_mod[coldict.SEXE_M]:
                        output[4, int(row["age"])] = float(row["poids"])
                    elif row[colnames.SEXE] == coldict.sexe_mod[coldict.SEXE_F]:
                        output[5, int(row["age"])] = float(row["poids"])       
            
            self.df_output_to_export = pd.DataFrame(output.transpose(), columns=coldict.dict_t_pop_init[self.current_input]) \
                                         .reset_index().rename(columns = {"index" : "age"})
                
        elif self.current_input == coldict.STR_taux_masc:
            output = np.zeros((1, 111), dtype = float)
            for _ , row in pd_output.iterrows():
                output[0, int(row["age"])] = float(row["taux_masc"])
            
            self.df_output_to_export = pd.DataFrame(output.transpose(), columns=["taux_masc"])

        elif self.current_input == coldict.STR_densite_travail:
            output = np.zeros((2, 111), dtype = float)
            for _ , row in pd_output.iterrows():
                if row[colnames.SEXE] == coldict.sexe_mod[coldict.SEXE_M]:
                    output[0, int(row["age"])] = float(row["densite_travail"])
                elif row[colnames.SEXE] == coldict.sexe_mod[coldict.SEXE_F]:
                    output[1, int(row["age"])] = float(row["densite_travail"])
    
            self.df_output_to_export = pd.DataFrame(output.transpose(), columns=["densite_H","densite_F"])

        elif self.current_input == coldict.STR_freq_conso:
            self.df_output_to_export = pd_output

        calculs.export_df(self.df_output_to_export, fname)

    def annuler_input(self):
        self.uit.table_params.setRowCount(0)
        self.uit.table_variables_requises.setRowCount(0)
        
        self.uit.button_annuler_input.setEnabled(False)
        self.uit.button_enregistrer_input.setEnabled(False)
        self.uit.button_fusionner.setEnabled(False)

        self.uit.button_selectionner_input.setEnabled(True)
    
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
