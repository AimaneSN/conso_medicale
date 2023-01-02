import os 

import colnames
from imports import *

###Types des variables pour les bases démographique, conso et ALD/ALC

SPARK_STRING = "StringType()"
SPARK_DATE = "DateType()"
SPARK_INT = "IntegerType()"
SPARK_DOUBLE = "DoubleType()"

assiette_types = {colnames.ID_BENEFICIAIRE_A : SPARK_STRING,
               colnames.ID_ADHERENT_A : SPARK_STRING,
               colnames.TYPE_ADHERENT : SPARK_STRING,
               colnames.TYPE_BENEFICIAIRE : SPARK_STRING,
               colnames.DNAISSANCE : SPARK_DATE,
               colnames.SEXE : SPARK_STRING,
               colnames.ASSIETTE_COTISATION : SPARK_DOUBLE,
               colnames.DATE_FIN_DO : SPARK_DATE,
               colnames.VILLE : SPARK_STRING,
               colnames.DATE_IMMATRICULATION : SPARK_DATE,
               colnames.NOMBRE_MOIS : SPARK_INT} 

conso_types = {colnames.ID_ADHERENT_C: SPARK_STRING,
            colnames.ID_BENEFICIAIRE_C : SPARK_STRING,
            colnames.RANG : SPARK_INT,
            colnames.DATE_DEBUT_SOIN : SPARK_DATE, 
            colnames.DATE_RECEPTION : SPARK_DATE,
            colnames.DATE_LIQUIDAITON : SPARK_DATE,
            colnames.DATE_PAIEMENT : SPARK_DATE,
            colnames.CODE_ACTE : SPARK_STRING,
            colnames.LIBELLE_ACTE : SPARK_STRING,
            colnames.SECTEUR_SOIN : SPARK_STRING,
            colnames.TYPE_ACTE : SPARK_STRING,
            colnames.COEFFICIENT : SPARK_INT,
            colnames.QUANTITE : SPARK_INT,
            colnames.PRIX_UNITAIRE : SPARK_DOUBLE,
            colnames.TAUX_REMBOURSEMENT : SPARK_INT,
            colnames.MONTANT_REMBOURSE : SPARK_DOUBLE,
            colnames.MONTANT_ENGAGE : SPARK_DOUBLE}

ald_types = {colnames.ID_ADHERENT_ALD : SPARK_STRING,
          colnames.ID_BENEFICIAIRE_ALD : SPARK_STRING,
          colnames.RANG_ALD : SPARK_INT,
          colnames.CODE_ALD_ALC : SPARK_STRING,
          colnames.DATE_DEBUT_ACCORD_ALD : SPARK_DATE,
          colnames.DATE_FIN_ACCORD_ALD : SPARK_DATE}

medic_types = {colnames.ID_ADHERENT_M : SPARK_STRING,
          colnames.ID_BENEFICIAIRE_M : SPARK_STRING,
          colnames.RANG_MEDIC : SPARK_INT,
          colnames.QUANTITE_MEDIC : SPARK_INT,
          colnames.TAUX_REMBOURSEMENT_M :SPARK_INT,
          colnames.PRIX_UNITAIRE_M : SPARK_DOUBLE,
          colnames.CLASSE_THERAPEUTIQUE : SPARK_STRING,
          colnames.TYPE_MEDICAMENT : SPARK_STRING,
          colnames.MONTANT_REMBOURSE_M : SPARK_DOUBLE,
          colnames.TYPE_ACTE_M : SPARK_STRING,
          colnames.CODE_MEDIC : SPARK_STRING
          }

###Indices des variables pour les bases démographique, conso et ALD/ALC

assiette_indices = {colnames.ID_BENEFICIAIRE_A : 0,
               colnames.ID_ADHERENT_A : 1,
               colnames.TYPE_ADHERENT : 3,
               colnames.TYPE_BENEFICIAIRE : 4,
               colnames.DNAISSANCE : 5,
               colnames.SEXE : 6,
               colnames.ASSIETTE_COTISATION : 8,
               colnames.DATE_FIN_DO : 9,
               colnames.VILLE : 10,
               colnames.DATE_IMMATRICULATION : 11,
               colnames.NOMBRE_MOIS : 12} 

conso_indices = {colnames.ID_ADHERENT_C : 0,
            colnames.ID_BENEFICIAIRE_C : 1,            
            colnames.RANG : 2,
            colnames.DATE_DEBUT_SOIN : 5, 
            colnames.DATE_RECEPTION: 4,
            colnames.DATE_LIQUIDAITON : 6,
            colnames.DATE_PAIEMENT : 7,
            colnames.CODE_ACTE : 9,
            colnames.LIBELLE_ACTE : 10,
            colnames.SECTEUR_SOIN : 20,
            colnames.TYPE_ACTE : 18,
            colnames.COEFFICIENT : 11,
            colnames.QUANTITE : 12,
            colnames.PRIX_UNITAIRE : 13,
            colnames.TAUX_REMBOURSEMENT : 14,
            colnames.MONTANT_REMBOURSE : 15,
            colnames.MONTANT_ENGAGE : 21}

ald_indices = {colnames.ID_ADHERENT_ALD : 0,
          colnames.ID_BENEFICIAIRE_ALD : 1,
          colnames.RANG_ALD : 2,
          colnames.CODE_ALD_ALC : 3,
          colnames.DATE_DEBUT_ACCORD_ALD : 4,
          colnames.DATE_FIN_ACCORD_ALD : 5}

medic_indices = {colnames.ID_ADHERENT_M : 0,
          colnames.ID_BENEFICIAIRE_M : 1,
          colnames.RANG_MEDIC : 2,
          colnames.QUANTITE_MEDIC : 3,
          colnames.TAUX_REMBOURSEMENT_M : 5,
          colnames.PRIX_UNITAIRE_M : 6,
          colnames.CLASSE_THERAPEUTIQUE : 7,
          colnames.TYPE_MEDICAMENT : 4,
          colnames.MONTANT_REMBOURSE_M : 8,
          colnames.TYPE_ACTE_M : 9,
          colnames.CODE_MEDIC : 10}

assiette_indices_ = dict.fromkeys(assiette_indices.keys())
conso_indices_ = dict.fromkeys(conso_indices.keys())
ald_indices_ = dict.fromkeys(ald_indices.keys())
medic_indices_ = dict.fromkeys(medic_indices.keys())

### Etat des variables (Utilisées = True, Non utilisées = False). Toutes les variables sont utilisées par défaut

assiette_etat = dict.fromkeys(assiette_indices, True)
conso_etat = dict.fromkeys(conso_indices, True)
ald_etat = dict.fromkeys(ald_indices, True)
medic_etat = dict.fromkeys(medic_indices, True)

### Modalités 

#Modalités pour "type_adherent"

ADH_ACTIF = "Actif"
ADH_RETRAITE = "Retraité/Invalide"
ADH_INACTIF = "Inactif"
ADH_ORPHELIN = "Orphelin"
ADH_VEUF = "Veuf"

type_adh_mod = {ADH_ACTIF : "Actif", 
                ADH_RETRAITE : "Retraité/Invalide",
                ADH_INACTIF : "Inactif",
                ADH_ORPHELIN : "Orphelin",
                ADH_VEUF : "Veuf"}

#Modalités pour "type_beneficiaire"

BENEF_ADHERENT = "Adhérent"
BENEF_CONJOINT = "Conjoint"
BENEF_ENFANT = "Enfant"
BENEF_CONJOINT_DM = "Conjoint droit maintenu"
BENEF_ENFANT_DM = "Enfant droit maintenu"

type_benef_mod = {BENEF_ADHERENT : "Adhérent",
                  BENEF_CONJOINT : "Conjoint",
                  BENEF_ENFANT : "Enfant",
                  BENEF_CONJOINT_DM : "Conjoint droit maintenu",
                  BENEF_ENFANT_DM : "Enfant droit maintenu"}

#Modalités pour "sexe"

SEXE_M = "M"
SEXE_F = "F"

sexe_mod = {SEXE_M : "M",
            SEXE_F : "F"}

#Modalités pour "type_acte"

TYPE_ACTE_AMBULATOIRE = "Ambulatoire"
TYPE_ACTE_HOSPITALISATION  = "Hospitalisation"

type_acte_mod = {TYPE_ACTE_AMBULATOIRE : "Ambulatoire",
                 TYPE_ACTE_HOSPITALISATION : "Hospitalisation"}

#Modalités pour "secteur_soin"

SECTEUR_PUBLIC = "Public"
SECTEUR_PRIVE = "Privé"

secteur_soin_mod = {SECTEUR_PUBLIC : "Public",
                    SECTEUR_PRIVE : "Privé"}

#Modalités pour pharmacie:

ACTE_PHARMA = "Pharmacie"

#Dictionnaire de toutes les modalités

vars_mod = {colnames.TYPE_ADHERENT : type_adh_mod,
            colnames.TYPE_BENEFICIAIRE : type_benef_mod,
            colnames.SEXE : sexe_mod,
            colnames.TYPE_ACTE : type_acte_mod,
            colnames.TYPE_ACTE_M : type_acte_mod,
            colnames.SECTEUR_SOIN : secteur_soin_mod}

#Formats des dates

# dates_formats_dict = {colnames.DNAISSANCE : "yyyy.MM.dd HH:mm:ss",
#               colnames.DATE_FIN_DO : "yyyyMMdd",
#               colnames.DATE_IMMATRICULATION : "yyyy-MM-dd",
#               colnames.DATE_DEBUT_SOIN : "yyyyMMdd",
#               colnames.DATE_RECEPTION : "yyyyMMdd",
#               colnames.DATE_LIQUIDAITON : "yyyyMMdd",
#               colnames.DATE_PAIEMENT : "yyyyMMdd",
#               colnames.DATE_DEBUT_ACCORD_ALD : "ddMMyyyy",
#               colnames.DATE_FIN_ACCORD_ALD : "ddMMyyyy"}

dates_formats_dict = {colnames.DNAISSANCE : "MM/dd/yyyy",
              colnames.DATE_FIN_DO : "yyyyMMdd",
              colnames.DATE_IMMATRICULATION : "MM/dd/yyyy",
              colnames.DATE_DEBUT_SOIN : "yyyyMMdd",
              colnames.DATE_RECEPTION : "yyyyMMdd",
              colnames.DATE_LIQUIDAITON : "yyyyMMdd",
              colnames.DATE_PAIEMENT : "yyyyMMdd",
              colnames.DATE_DEBUT_ACCORD_ALD : "yyyyMMdd",
              colnames.DATE_FIN_ACCORD_ALD : "yyyyMMdd"}

###Identifiants des inputs

STR_table_1_a_s = "Table 1 a,s"
STR_table_2_f_c = "Table 2 f,c"
STR_table_3 = "Table 3 autre"
STR_table_freq_conso = "Table frequence conso"
STR_table_pop_initiale = "Table population initiale"

#Inputs de la table 1 a,s
STR_densite_travail = "Densite de travail par age et sexe (hors nouveaux cotisants)"
STR_taux_masc = "Taux de masculinite des beneficiaires par age (population des enfants)"
STR_taux_prevalence_ALD = "Taux de prevalence des ALD par age et sexe"

dict_t_1 = {STR_densite_travail : ["densite_travail_H", "densite_travail_F"],
             STR_taux_masc : ["taux_masc"],
             STR_taux_prevalence_ALD : None}

#Input de la table 3
STR_distrib_salaires_pensions_moy_actifs = "Distribution des salaires et des pensions autour de la moyenne (actifs)"
STR_distrib_salaires_pensions_moy_pensionnes = "Distribution des salaires et des pensions autour de la moyenne (pensionnes)"

dict_t_3 = dict.fromkeys([STR_distrib_salaires_pensions_moy_actifs, STR_distrib_salaires_pensions_moy_pensionnes])

#Input de la table frequence conso
STR_freq_conso = "Frequence de consommation par categorie demographique et libelle acte"
STR_freq_conso_medic = "Frequence de consommation par categorie demographique et libelle medicament"

dict_t_f_c = dict.fromkeys([STR_freq_conso, STR_freq_conso_medic])

#Inputs de la table population initiale
STR_input_effectifs_salaires_actifs = "Effectifs et salaires moyens des actifs cotisants par age et sexe"
STR_input_nvx_actifs = "Effectifs et salaires moyens des nouveaux actifs par age et sexe"
STR_input_effectifs_pensions_retraites = "Effectifs et pensions moyennes des retraites par age et sexe"
STR_input_effectifs_pensions_conjoints_surv = "Effectifs et pensions moyennes des conjoints survivants par age et sexe"
STR_input_effectifs_dormants = "Effectifs des dormants par age et sexe"

dict_t_pop_init = {STR_input_effectifs_salaires_actifs : ["eff_H","eff_F","salaire_moy_H","salaire_moy_F", "_", "_"],
                   STR_input_nvx_actifs : ["eff_H", "eff_F", "salaire_moy_H", "salaire_moy_F", "poids_H", "poids_F"],
                   STR_input_effectifs_pensions_retraites : ["eff_H","eff_F","pension_moy_H","pension_moy_F", "_", "_"],
                   STR_input_effectifs_pensions_conjoints_surv : ["eff_H","eff_F","pension_moy_H","pension_moy_F", "_", "_"],
                   STR_input_effectifs_dormants : None}

#Dictionnaire de tous les inputs
dict_inputs = {STR_table_1_a_s : dict_t_1,
               STR_table_3 : dict_t_3,
               STR_table_freq_conso : dict_t_f_c,
               STR_table_pop_initiale : dict_t_pop_init}

#Matrices de consommation

MATRICE_CONSO = "Matrice de consommations hors pharmacie"
MATRICE_PHARMA = "Matrice de consommations (pharmacie uniquement)"
