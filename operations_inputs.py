
import findspark
findspark.init()
import pyspark
import openpyxl
import os
from pyspark.sql import SparkSession, Row, HiveContext, Window
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

import datetime
import colnames

import coldict
import readfile
import utils
import calculs

import  pyspark.sql.functions  as F
import pandas as pd

def suppression_manquants(df, vars_requises : list):

       for var in vars_requises:
              if var not in df.columns:
                     return f"La variable requise {var} n'est pas dans la base selectionnee"

       N_initial = df.count()

       for colname in vars_requises:
              df = df.filter(F.col(colname).isNotNull())

       N_final = df.count()

       nb_manquants = N_initial - N_final

       return df, nb_manquants
       
def input_variables_demo(df, annee : int):
       
       if colnames.DATE_IMMATRICULATION in df.columns:
              #Extraction de l'année (annee_imm) et du mois (mois_imm) d'immatriculation
              df = df.withColumn("annee_imm", F.year(df[colnames.DATE_IMMATRICULATION]).cast(IntegerType())) \
              .withColumn("mois_imm", F.month(df[colnames.DATE_IMMATRICULATION]).cast(IntegerType()))

       #Création de variables binaires pour le calcul des inputs (0 ou 1)
       if set([colnames.TYPE_ADHERENT, colnames.TYPE_BENEFICIAIRE, "annee_imm"]).issubset(df.columns):
              df = df.withColumn("adherent_actif", F.when( (df[colnames.TYPE_BENEFICIAIRE] == coldict.BENEF_ADHERENT) & (df[colnames.TYPE_ADHERENT] == coldict.ADH_ACTIF), 1).otherwise(0)) 
              df = df.withColumn("adherent_nvx_actif", F.when( (df.adherent_actif == 1) & (df.annee_imm == annee) , 1).otherwise(0)) \
                     .withColumn("adherent_ancien_actif", F.when( (df.adherent_actif == 1) & (df.annee_imm < annee) , 1).otherwise(0)) \
                     .withColumn("adherent_retraite", F.when( (df[colnames.TYPE_BENEFICIAIRE] == coldict.BENEF_ADHERENT) & (df[colnames.TYPE_ADHERENT] == coldict.ADH_RETRAITE), 1).otherwise(0)) \
                     .withColumn("adherent_conjoint_survivant", F.when( (df[colnames.TYPE_BENEFICIAIRE] == coldict.BENEF_ADHERENT) & (df[colnames.TYPE_ADHERENT] == coldict.ADH_VEUF), 1).otherwise(0)) 

              df = df.withColumn("type_benef", F.when(F.col(colnames.TYPE_BENEFICIAIRE)==coldict.BENEF_ADHERENT, F.col(colnames.TYPE_ADHERENT))
                                          .otherwise(F.col(colnames.TYPE_BENEFICIAIRE)))

       #Annualisation de l'assiette de cotisation
       if colnames.NOMBRE_MOIS in df.columns:
              df = df.withColumn("assiette_cotisation_annualisee", F.when( (F.col(colnames.NOMBRE_MOIS).isNotNull()) & (F.col(colnames.NOMBRE_MOIS) != 0), \
                                          F.col(colnames.ASSIETTE_COTISATION) * 12 / F.least(F.col(colnames.NOMBRE_MOIS), F.lit(12)))
                                          .otherwise(0))

       elif set(["adherent_nvx_actif", "adherent_ancien_actif"]).issubset(df.columns):
                     df = df.withColumn("assiette_cotisation_annualisee", F.when(df.adherent_nvx_actif == 1, F.col(colnames.ASSIETTE_COTISATION) * 12 / F.greatest(F.lit(1), F.lit(12) - df.mois_imm))
                                                                           .when(df.adherent_ancien_actif == 1, df[colnames.ASSIETTE_COTISATION]))

       #Création de la variable age
       if colnames.DNAISSANCE in df.columns:
              df = df.withColumn("age", F.floor(F.months_between(F.lit(datetime.datetime(annee,12,31)), F.col(colnames.DNAISSANCE))/12))

       return df

###Inputs population initiale

def input_effectifs_salaires_actifs(df):
       #Variables requises : type adhérent, type bénéficiaire, age, sexe, assiette

       df, nb_m = suppression_manquants(df.filter(df.adherent_actif == 1), ["age", colnames.SEXE])

       #Effectifs des actifs cotisants par âge et sexe
       df = df.groupBy(df.age, colnames.SEXE) \
              .agg(F.count(df[colnames.ID_BENEFICIAIRE_A]).alias("effectifs_actifs_cotisants"),
                   F.mean(df.assiette_cotisation_annualisee).alias("salaire_moyen"))

       tot = df.groupBy().sum("effectifs_actifs_cotisants").take(1)[0][0]

       df = df.withColumn("effectifs_actifs_cotisants", df["effectifs_actifs_cotisants"] + (nb_m/tot) * df["effectifs_actifs_cotisants"])

       #df_resultat = pd.pivot_table(df, values = ["effectifs_actifs_cotisants", "salaire_moyen"], index = "age", columns = "sexe")

       return df

def input_effectifs_dormants(df, annee : int):
       #Variables requises : age, sexe
       
       df, nb_m = suppression_manquants(df, ["age", colnames.SEXE])

       #Effectifs des actifs cotisants par âge et sexe
       df = df.groupBy(df.age, colnames.SEXE).agg(F.count(df["age"]).alias("effectifs_dormants"))

       tot = df.groupBy().sum("effectifs_dormants").take(1)[0][0]
       
       df = df.withColumn("effectifs_dormants", df["effectifs_dormants"] + (nb_m/tot) * df["effectifs_dormants"])

       return df

def input_nvx_actifs(df, annee : int, age_min : int, age_max : int):
       #Variables requises : type adhérent, type bénéficiaire, date immatriculation, age, sexe, assiette

       df = df.filter( (df.adherent_nvx_actif == 1) & (df.age.between(age_min, age_max)) )
       print(df.count())
       df, nb_m = suppression_manquants(df, ["age", colnames.SEXE])

       #Effectif total
       tot = df.count()

       #Poids et salaire moyen par âge et sexe
       df = df.filter( (df.adherent_nvx_actif == 1) & (df.age.between(age_min, age_max)) ) \
           .groupBy(df.age, colnames.SEXE) \
           .agg(F.count(df[colnames.ID_BENEFICIAIRE_A]).alias("effectif"), F.mean(df[colnames.ASSIETTE_COTISATION]).alias("salaire_moyen")) 

       df = df.withColumn("effectif", df["effectif"] + (nb_m/tot) * df["effectif"])

       df = df.withColumn("poids", df.effectif/(tot+nb_m)) 
       
       return df 

def input_densite_travail(df, annee : int):
       #Variables requises : type adhérent, type bénéficiaire, date d'immatriculation, age, sexe

       df = df.filter(df.adherent_ancien_actif == 1)

       if colnames.NOMBRE_MOIS in df.columns:   
              df = df.withColumn("r_mois", df[colnames.NOMBRE_MOIS]/12)

       else:
              df = df.withColumn("r_mois", F.lit(1))
       
       df, _ = suppression_manquants(df, ["age", colnames.SEXE])
       
       #Calcul de la densité de travail par âge et sexe
       df = df.groupBy(df.age, colnames.SEXE).agg(F.mean(df.r_mois).alias("densite_travail"))

       #df = pd.pivot_table(df, values = "densite_travail", index = "age", columns = "sexe")
       
       return df

def input_taux_masc(df):
       #Variables requises : type bénéficiaire, age, sexe

       #Taux de masculinité (population des enfants)

       df, _ = suppression_manquants(df.filter(df[colnames.TYPE_BENEFICIAIRE] == "Enfant"), ["age"]) #Remplacer "Enfant" par la modalité 

       df = df.groupBy(df.age).agg(F.count(F.when(df[colnames.SEXE]=="M",df[colnames.ID_BENEFICIAIRE_A])).alias("N_H"),
                                   F.count(df[colnames.ID_BENEFICIAIRE_A]).alias("N")) 
                                                                                 
       df = df.withColumn("taux_masc", df.N_H/df.N)

       #df = pd.pivot_table(df, values = "taux_masc", index = "age")

       return df

def input_effectifs_pensions_retraites(df):
       #Variables requises : type adhérent, type bénéficiaire, age, sexe, assiette

       df, nb_m = suppression_manquants(df.filter(df.adherent_retraite == 1), ["age", colnames.SEXE])

       #Effectifs et pensions des retraités par âge et sexe
       df = df.groupBy(df.age, colnames.SEXE).agg(F.count(df[colnames.ID_BENEFICIAIRE_A]).alias("effectifs_retraites"),
                                                  F.mean(df[colnames.ASSIETTE_COTISATION]).alias("pension_moyenne"))
       
       tot = df.groupBy().sum("effectifs_retraites").take(1)[0][0]

       df = df.withColumn("effectifs_retraites", df["effectifs_retraites"] + (nb_m/tot) * df["effectifs_retraites"])

       #df = pd.pivot_table(df, values = ["effectifs_retraites", "pension_moyenne"], index = "age", columns = "sexe")

       return df

def input_effectifs_pensions_conjoints_surv(df):
       #Variables requises : type adhérent, type bénéficiaire, age, sexe, assiette

       df, nb_m = suppression_manquants(df.filter(df.adherent_conjoint_survivant == 1), ["age", colnames.SEXE])

       #Effectifs et pensions des retraités par âge et sexe
       df = df.groupBy(df.age, colnames.SEXE).agg(F.count(df[colnames.ID_BENEFICIAIRE_A]).alias("effectifs_conjoints_survivants"),
                                                  F.mean(df[colnames.ASSIETTE_COTISATION]).alias("pension_moyenne"))
       
       tot = df.groupBy().sum("effectifs_conjoints_survivants").take(1)[0][0]

       df = df.withColumn("effectifs_conjoints_survivants", df["effectifs_conjoints_survivants"] + (nb_m/tot) * df["effectifs_conjoints_survivants"])

       #df = pd.pivot_table(df, values = ["effectifs_conjoints_survivants", "pension_moyenne"], index = "age", columns = "sexe")

       return df

def input_taux_prevalence_ALD(df,  L_manquants : list):
       
       nm_ald = L_manquants[0]

       if len(L_manquants) > 1:
              eff_ALD = L_manquants[1]
       else:
              eff_ALD = df.filter(df["is_ALD"] == 1).count()
       

       df, _ = suppression_manquants(df, ["age", colnames.SEXE])

       df = df.groupBy(df.age, colnames.SEXE).agg(F.count(F.when(df["is_ALD"] == 1, 1)).alias("N_ALD"),
                                                 F.count(F.when(df["is_ALD"] == 0, 1)).alias("N_NALD"))
       
       df = df.withColumn("N_ALD", df["N_ALD"] +  nm_ald * (df["N_ALD"]/eff_ALD))

       df = df.withColumn("taux_prevalence_ALD", df["N_ALD"]/(df["N_ALD"] + df["N_NALD"]))

       df = df.withColumn("taux_nonALD_nvx_entrants", 1 - df["taux_prevalence_ALD"])

       return df

def input_distrib_assiettes(df):
       
       df, _ = suppression_manquants(df.filter( (df.adherent_actif == 1) | (df.adherent_retraite == 1) |
                                                                             (df.adherent_conjoint_survivant == 1)), ["age", colnames.SEXE])
       
       partition = Window.partitionBy("age", colnames.SEXE)

       df_actif = df.filter(df.adherent_actif == 1).withColumn("assiette_moy_age_sexe", F.mean(colnames.ASSIETTE_COTISATION).over(partition))
       df_pensionne = df.filter( (df.adherent_retraite == 1) | (df.adherent_conjoint_survivant == 1) ).withColumn("pension_moy_age_sexe", F.mean(colnames.ASSIETTE_COTISATION).over(partition))

       df_actif = df_actif.withColumn("r_assiette", F.round(100 * df_actif[colnames.ASSIETTE_COTISATION] / df_actif["assiette_moy_age_sexe"], 0))
       df_pensionne =  df_pensionne.withColumn("r_pension", F.round(100 * df_pensionne[colnames.ASSIETTE_COTISATION] / df_pensionne["pension_moy_age_sexe"], 0))

       df_actif = df_actif.withColumn("r_assiette", F.least(F.lit(300), 5 * F.round(df_actif["r_assiette"]/5)))
       df_pensionne = df_pensionne.withColumn("r_pension", F.least(F.lit(300), 5 * F.round(df_pensionne["r_pension"]/5)))
              
       partition_sexe = Window.partitionBy(colnames.SEXE)

       df_actif = df_actif.withColumn("eff_sexe", F.count(colnames.SEXE).over(partition_sexe))
       df_actif = df_actif.withColumn("r_sexe", 1/df_actif["eff_sexe"])

       df_pensionne = df_pensionne.withColumn("eff_sexe", F.count(colnames.SEXE).over(partition_sexe))
       df_pensionne = df_pensionne.withColumn("r_sexe", 1/df_pensionne["eff_sexe"])

       df_actif = df_actif.groupBy("r_assiette", colnames.SEXE).agg({"r_sexe" : "sum"}).sort("r_assiette", ascending=True)
       df_pensionne = df_pensionne.groupBy("r_pension", colnames.SEXE).agg({"r_sexe" : "sum"}).sort("r_pension", ascending=True)

       return df_actif, df_pensionne

def effectifs_cat_demo(df):
       
       df = df.groupBy(colnames.SEXE, "age_q", "is_ALD").count().orderBy(["is_ALD", colnames.SEXE, "age_q"], ascending = [True, False, True])
       df_pd = df.withColumn("cat_demo", F.concat("is_ALD", F.lit("_"), colnames.SEXE, F.lit("_"), "age_q")).drop(colnames.SEXE, "age_q", "is_ALD").toPandas().reset_index()
       L = []
       for is_ald in [0, 1]:
              for sexe in [coldict.sexe_mod[coldict.SEXE_M], coldict.sexe_mod[coldict.SEXE_F]]:
                     for age_q in range(1, 23):
                            cat_demo = str(is_ald) + "_" + sexe + "_" + str(age_q)
                            L.append(cat_demo)
       
       dict_demo = {k : 0 for k in L}
       for index, row in df_pd.iterrows():
              if row['cat_demo'] in dict_demo.keys():
                     dict_demo[row['cat_demo']] = row['count']

       return dict_demo

def matrice_conso(df, actes_pharma : list): 

    df = df.filter( (F.col(colnames.SEXE).isNotNull()) & (F.col(colnames.PRIX_UNITAIRE) > 0) & (F.col(colnames.QUANTITE) > 0) & (F.col(colnames.TAUX_REMBOURSEMENT) > 0))

    df = df.filter(~ F.col(colnames.CODE_ACTE).isin(actes_pharma)) #Remplacer PH7 par la modalité "Pharmacie"

    montant_rembourse_pre = df.agg(F.sum(F.col(colnames.MONTANT_REMBOURSE))).rdd.flatMap(lambda x: x).collect()[0]
    
    if colnames.TYPE_ACTE in df.columns:
        df = df.withColumn("nb_Amb_", F.when(F.col(colnames.TYPE_ACTE) == coldict.TYPE_ACTE_AMBULATOIRE, F.col("quantite_calcule")).otherwise(0)) 
    else:
        df = df.withColumn("nb_Amb_", F.lit(0.8))

    if colnames.MONTANT_ENGAGE in df.columns:
        df_ = df.groupBy(colnames.CODE_ACTE, colnames.PRIX_UNITAIRE, colnames.TAUX_REMBOURSEMENT, 
                     "is_ALD", colnames.SEXE, "age_q").agg({"quantite_calcule" : "sum", colnames.MONTANT_ENGAGE : "sum", "nb_Amb_" : "sum"}) 
    else:
        df_ = df.groupBy(colnames.CODE_ACTE, colnames.PRIX_UNITAIRE, colnames.TAUX_REMBOURSEMENT, 
                     "is_ALD", colnames.SEXE, "age_q").agg({"quantite_calcule" : "sum", colnames.MONTANT_REMBOURSE : "sum", "nb_Amb_" : "sum"}) \
                    .withColumnRenamed("sum("+colnames.MONTANT_REMBOURSE+")", "sum("+colnames.MONTANT_ENGAGE+")")

    df_ = df_.withColumn("part_amb_", F.col("sum(nb_Amb_)")/F.col("sum(quantite_calcule)"))
    df_ = df_.withColumn("montant_engage_moy_", F.col("sum("+colnames.MONTANT_ENGAGE+")")/F.col("sum(quantite_calcule)"))
    
    df_ = df_.orderBy([colnames.CODE_ACTE, "is_ALD", colnames.SEXE, "age_q"], ascending = [True, True, False, True])
    
    df_ = df_.withColumn("part_amb_qt_", F.col("part_amb_") * F.col("sum(quantite_calcule)")) \
             .withColumn("montant_engage_qt_", F.col("montant_engage_moy_") * F.col("sum(quantite_calcule)"))

    w1 = Window.partitionBy(colnames.CODE_ACTE, colnames.PRIX_UNITAIRE, colnames.TAUX_REMBOURSEMENT)

    df_ = df_.withColumn("part_amb", F.sum("part_amb_qt_").over(w1)/F.sum("sum(quantite_calcule)").over(w1)) \
             .withColumn("montant_engage_moy", F.sum("montant_engage_qt_").over(w1)/F.sum("sum(quantite_calcule)").over(w1))
    
    for is_ald in [0, 1]:
        for sexe in [coldict.sexe_mod[coldict.SEXE_M], coldict.sexe_mod[coldict.SEXE_F]]:
            for age_q in range(1, 23):
                col_name_ = str(is_ald) + "_" + sexe + "_" + str(age_q)
                df_ = df_.withColumn(col_name_, F.when( (F.col("is_ALD") == is_ald) & (F.col(colnames.SEXE) == sexe) & (F.col("age_q") == age_q),
                                                                  F.col("sum(quantite_calcule)")).otherwise(0))

    exprs = [F.sum(x).alias(x) for x in df_.columns[15:]]
    df_ = df_.groupBy([colnames.CODE_ACTE, colnames.PRIX_UNITAIRE, colnames.TAUX_REMBOURSEMENT]) \
             .agg(F.first("part_amb").alias("part_amb"),
                  F.first("montant_engage_moy").alias("montant_engage_moy"),
                  F.sum("sum(quantite_calcule)").alias("qt_totale"),
                  *exprs) \
             .orderBy([colnames.CODE_ACTE, colnames.PRIX_UNITAIRE, colnames.TAUX_REMBOURSEMENT], ascending = [True, True, True])
    
    montant_rembourse_post = df_.withColumn("montant_rembourse", F.col(colnames.PRIX_UNITAIRE) * F.col(colnames.TAUX_REMBOURSEMENT) * F.col("qt_totale")) \
                                .agg(F.sum(F.col("montant_rembourse"))).rdd.flatMap(lambda x: x).collect()[0]

    return df_, montant_rembourse_pre, montant_rembourse_post

def matrice_conso_medic(df):

    df = df.filter( (F.col(colnames.SEXE).isNotNull()) & (F.col(colnames.PRIX_UNITAIRE_M) > 0) & (F.col(colnames.QUANTITE_MEDIC) > 0) & (F.col(colnames.TAUX_REMBOURSEMENT_M) > 0))

    if colnames.MONTANT_REMBOURSE_M not in df.columns:
        df = df.withColumn(colnames.MONTANT_REMBOURSE_M, F.col(colnames.PRIX_UNITAIRE_M) * F.col(colnames.TAUX_REMBOURSEMENT_M) * F.col("quantite_calcule"))
    
    montant_rembourse_pre = df.agg(F.sum(F.col(colnames.MONTANT_REMBOURSE_M))).rdd.flatMap(lambda x: x).collect()[0]
    
    if colnames.TYPE_ACTE in df.columns:
        df = df.withColumn("nb_Amb_", F.when(F.col(colnames.TYPE_ACTE) == coldict.TYPE_ACTE_AMBULATOIRE, F.col("quantite_calcule")).otherwise(0))
    else:
        df = df.withColumn("nb_Amb_", F.lit(1))

    if colnames.MONTANT_ENGAGE in df.columns:
        df_ = df.groupBy(colnames.CODE_MEDIC, colnames.PRIX_UNITAIRE_M, colnames.TAUX_REMBOURSEMENT_M, 
                     "is_ALD", colnames.SEXE, "age_q").agg({"quantite_calcule" : "sum", colnames.MONTANT_ENGAGE : "sum", "nb_Amb_" : "sum"})
    else:
        df_ = df.groupBy(colnames.CODE_MEDIC, colnames.PRIX_UNITAIRE_M, colnames.TAUX_REMBOURSEMENT_M, 
                     "is_ALD", colnames.SEXE, "age_q").agg({"quantite_calcule" : "sum", colnames.MONTANT_REMBOURSE_M : "sum", "nb_Amb_" : "sum"}) \
                    .withColumnRenamed("sum("+colnames.MONTANT_REMBOURSE_M+")", "sum("+colnames.MONTANT_ENGAGE+")")

    df_ = df_.withColumn("part_amb_", F.col("sum(nb_Amb_)")/F.col("sum(quantite_calcule)"))
    df_ = df_.withColumn("montant_engage_moy_", F.col("sum("+colnames.MONTANT_ENGAGE+")")/F.col("sum(quantite_calcule)"))
    
    df_ = df_.orderBy([colnames.CODE_MEDIC, "is_ALD", colnames.SEXE, "age_q"], ascending = [True, True, False, True])
    
    df_ = df_.withColumn("part_amb_qt_", F.col("part_amb_") * F.col("sum(quantite_calcule)")) \
             .withColumn("montant_engage_qt_", F.col("montant_engage_moy_") * F.col("sum(quantite_calcule)"))

    w1 = Window.partitionBy(colnames.CODE_MEDIC, colnames.PRIX_UNITAIRE_M, colnames.TAUX_REMBOURSEMENT_M)

    df_ = df_.withColumn("part_amb", F.sum("part_amb_qt_").over(w1)/F.sum("sum(quantite_calcule)").over(w1)) \
             .withColumn("montant_engage_moy", F.sum("montant_engage_qt_").over(w1)/F.sum("sum(quantite_calcule)").over(w1))
    
    for is_ald in [0, 1]:
        for sexe in [coldict.sexe_mod[coldict.SEXE_M], coldict.sexe_mod[coldict.SEXE_F]]:
            for age_q in range(1, 23):
                col_name_ = str(is_ald) + "_" + sexe + "_" + str(age_q) 
                df_ = df_.withColumn(col_name_, F.when( (F.col("is_ALD") == is_ald) & (F.col(colnames.SEXE) == sexe) & (F.col("age_q") == age_q),
                                                                  F.col("sum(quantite_calcule)")).otherwise(0))

    exprs = [F.sum(x).alias(x) for x in df_.columns[15:]]
    df_ = df_.groupBy([colnames.CODE_MEDIC, colnames.PRIX_UNITAIRE_M, colnames.TAUX_REMBOURSEMENT_M]) \
             .agg(F.first("part_amb").alias("part_amb"),
                  F.first("montant_engage_moy").alias("montant_engage_moy"),
                  F.sum("sum(quantite_calcule)").alias("qt_totale"),
                  *exprs) \
             .orderBy([colnames.CODE_MEDIC, colnames.PRIX_UNITAIRE_M, colnames.TAUX_REMBOURSEMENT_M], ascending = [True, True, True])
    
    montant_rembourse_post = df_.withColumn("montant_rembourse", F.col(colnames.PRIX_UNITAIRE_M) * F.col(colnames.TAUX_REMBOURSEMENT_M) * F.col("qt_totale")) \
                                .agg(F.sum(F.col("montant_rembourse"))).rdd.flatMap(lambda x: x).collect()[0]

    return df_, montant_rembourse_pre, montant_rembourse_post

def matrice_conso_ph(df, actes_pharma : list): 

    df = df.filter( (F.col(colnames.SEXE).isNotNull()) & (F.col(colnames.PRIX_UNITAIRE) > 0) & (F.col(colnames.QUANTITE) > 0) & (F.col(colnames.TAUX_REMBOURSEMENT) > 0))

    df = df.filter(F.col(colnames.CODE_ACTE).isin(actes_pharma)) #Remplacer PH7 par la modalité "Pharmacie"

    montant_rembourse_pre = df.agg(F.sum(F.col(colnames.MONTANT_REMBOURSE))).rdd.flatMap(lambda x: x).collect()[0]

    if colnames.TYPE_ACTE in df.columns:
        df = df.withColumn("nb_Amb_", F.when(F.col(colnames.TYPE_ACTE) == "Ambulatoire", F.col("quantite_calcule")).otherwise(0)) #Modifier ambulatoire
    else:
        df = df.withColumn("nb_Amb_", F.lit(1))

    if colnames.MONTANT_ENGAGE in df.columns:
        df_ = df.groupBy(colnames.CODE_ACTE, colnames.PRIX_UNITAIRE, colnames.TAUX_REMBOURSEMENT, 
                     "is_ALD", colnames.SEXE, "age_q").agg({"quantite_calcule" : "sum", colnames.MONTANT_ENGAGE : "sum", "nb_Amb_" : "sum"}) #Modifier montant_engage par colnames.MONTANT_ENGAGE
    
    else:
        df_ = df.groupBy(colnames.CODE_ACTE, colnames.PRIX_UNITAIRE, colnames.TAUX_REMBOURSEMENT, 
                     "is_ALD", colnames.SEXE, "age_q").agg({"quantite_calcule" : "sum", colnames.MONTANT_REMBOURSE : "sum", "nb_Amb_" : "sum"}) \
            .withColumnRenamed("sum("+colnames.MONTANT_REMBOURSE+")", "sum("+colnames.MONTANT_ENGAGE+")")
    
    df_ = df_.withColumn("part_amb_", F.col("sum(nb_Amb_)")/F.col("sum(quantite_calcule)"))
    df_ = df_.withColumn("montant_engage_moy_", F.col("sum("+colnames.MONTANT_ENGAGE+")")/F.col("sum(quantite_calcule)"))
    
    df_ = df_.orderBy([colnames.CODE_ACTE, "is_ALD", colnames.SEXE, "age_q"], ascending = [True, True, False, True])
    
    df_ = df_.withColumn("part_amb_qt_", F.col("part_amb_") * F.col("sum(quantite_calcule)")) \
             .withColumn("montant_engage_qt_", F.col("montant_engage_moy_") * F.col("sum(quantite_calcule)"))

    w1 = Window.partitionBy(colnames.CODE_ACTE, colnames.PRIX_UNITAIRE, colnames.TAUX_REMBOURSEMENT)

    df_ = df_.withColumn("part_amb", F.sum("part_amb_qt_").over(w1)/F.sum("sum(quantite_calcule)").over(w1)) \
             .withColumn("montant_engage_moy", F.sum("montant_engage_qt_").over(w1)/F.sum("sum(quantite_calcule)").over(w1))
    
    for is_ald in [0, 1]:
        for sexe in [coldict.sexe_mod[coldict.SEXE_M], coldict.sexe_mod[coldict.SEXE_F]]:
            for age_q in range(1, 23):
                col_name_ = str(is_ald) + "_" + sexe + "_" + str(age_q)
                df_ = df_.withColumn(col_name_, F.when( (F.col("is_ALD") == is_ald) & (F.col(colnames.SEXE) == sexe) & (F.col("age_q") == age_q),
                                                                  F.col("sum(quantite_calcule)")).otherwise(0))

    exprs = [F.sum(x).alias(x) for x in df_.columns[15:]]
    df_ = df_.groupBy([colnames.CODE_ACTE, colnames.PRIX_UNITAIRE, colnames.TAUX_REMBOURSEMENT]) \
             .agg(F.first("part_amb").alias("part_amb"),
                  F.first("montant_engage_moy").alias("montant_engage_moy"),
                  F.sum("sum(quantite_calcule)").alias("qt_totale"),
                  *exprs) \
             .orderBy([colnames.CODE_ACTE, colnames.PRIX_UNITAIRE, colnames.TAUX_REMBOURSEMENT], ascending = [True, True, True])

    df_ = df_.withColumn("prix_unitaire_qt", F.col(colnames.PRIX_UNITAIRE) * F.col("qt_totale")) \
             .withColumn("prix_engage_qt", F.col("montant_engage_moy") * F.col("qt_totale")) \
             .withColumn("part_amb_qt", F.col("part_amb") * F.col("qt_totale"))

    exprs2 = [F.sum(x).alias(x) for x in df_.columns[5:94]]
    df_ = df_.groupBy(colnames.TAUX_REMBOURSEMENT).agg((F.sum(F.col("prix_unitaire_qt"))/F.sum(F.col("qt_totale"))).alias("prix_unitaire_moy"),
                                                       (F.sum(F.col("prix_engage_qt"))/F.sum(F.col("qt_totale"))).alias("prix_engage_moy"),
                                                       (F.sum(F.col("part_amb_qt"))/F.sum(F.col("qt_totale"))).alias("part_amb_moy"),
                                                       *exprs2) \
                                                  .withColumnRenamed(colnames.CODE_ACTE, coldict.ACTE_PHARMA) #Change pH7 en coldict.ACTE_PHARMA
    
    columns = [df_.columns[-1]] + df_.columns[:-1]
    df_ = df_.select(columns)
    
    montant_rembourse_post = df_.withColumn("montant_rembourse", F.col("prix_unitaire_moy") * F.col(colnames.TAUX_REMBOURSEMENT) * F.col("qt_totale")) \
                                .agg(F.sum(F.col("montant_rembourse"))).rdd.flatMap(lambda x: x).collect()[0]

    return df_, montant_rembourse_pre, montant_rembourse_post

def freq_conso(df, dict_eff : dict):
    if "0_M_1" in df.columns:
        index = df.columns.index("0_M_1")
    else:
        error = "Categories introuvables"
        return
    
    for colname in df.columns[index:]:
        if dict_eff[colname] != 0:
            df = df.withColumn(colname,(F.col(colname)/F.lit(dict_eff[colname])) * 1000)
        else:
            df = df.withColumn(colname, F.lit(0))
    
    return df

