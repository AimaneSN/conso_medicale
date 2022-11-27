
import findspark
findspark.init()
import pyspark
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
import  pyspark.sql.functions  as F
import pandas as pd

def nb_manquants(df):

    dict_m =  dict.fromkeys(df.columns) #nombre d'observations manquantes par colonne
    dict_describe = dict.fromkeys(df.columns) #statistiques descriptives par colonne

    tot = df.count()
    for column in dict_m.keys():
        column_m = df.filter(F.col(column).isNull()).count()
        dict_m[column] = [column_m, column_m/tot] 
        if column_m/tot > 0.025:
            print(f"Observations manquantes pour la variable {column} supérieures à 2.5% ( {round(column_m/tot, 3)*100}%)")
        
        #types = df.dtypes[dict_base[column]][1]
        #if types in ['int', 'double', 'bigint']:
         #   dict_describe[column] = utils.describe_column(df, column)
        
    return dict_m

def age_operations(df, year : int, dormant : bool):
    
    #Création de la variable AGE
    df = df.withColumn("age", F.floor(F.months_between(F.lit(datetime.datetime(year,12,31)),
                                                        df.dnaissance)/F.lit(12)))
    
    agemoy = df.groupBy().avg("age").take(1)[0][0]
    df = df.withColumn("age_moy", F.lit(agemoy))
        
    #Correction des âges
    if dormant == 0:
        
        df = df.withColumn("type_benef", F.when(F.col(colnames.TYPE_BENEFICIAIRE)=="Adhérent", F.col("type_adherent"))
                                                         .otherwise(F.col(colnames.TYPE_BENEFICIAIRE)))
                                                
        agep = Window.partitionBy(colnames.TYPE_ADHERENT, colnames.TYPE_BENEFICIAIRE, colnames.SEXE)
        df = df.withColumn("age_0",F.mean(F.when( (F.col("age") < 110) & (F.col("age") > 0), F.col("age"))).over(agep))

        type_a_sexep = Window.partitionBy(colnames.TYPE_ADHERENT, colnames.SEXE)
        df = df.withColumn("age_1", F.mean(F.when( (F.col("age") < 110) & (F.col("age") > 0), F.col("age"))).over(type_a_sexep))

        sexep = Window.partitionBy(colnames.SEXE)
        df = df.withColumn("age_2", F.mean(F.when( (F.col("age") < 110) & (F.col("age") > 0), F.col("age"))).over(sexep))

        df = df.withColumn("age_corr", F.when( (df.age.isNull()) | (df.age < 0) | (df.age > 110), F.when((df.age_0.isNull()) | (df.age_0 < 0) | (df.age > 110),
                                        F.when((df.age_1.isNull()) | (df.age_1 < 0) | (df.age > 110), df.age_2 ).otherwise(df.age_1) ).otherwise(df.age_0) ).otherwise(df.age))

        df = df.withColumn("age_corr", F.when((df.age < 15) & (F.col("type_benef")=="Actif"), 15) \
                                                            .when((df.age > 70) & (df.type_benef == "Actif"), df.age_0) \
                                                            .when((df.age < 16) & (df.type_benef == "Veuf"), 16) \
                                                            .when(df.age > 110, df.age_0) \
                                                            .otherwise(F.col("age_corr")))
    else :
        sexep = Window.partitionBy(colnames.SEXE)
        df = df.withColumn("age_corr", F.when(F.col(colnames.SEXE).isNotNull(), F.floor(F.mean("age").over(sexep))).otherwise(F.col("age_moy")))
    
    #Création de la variable age_q (age quinquennal)
    df = df.withColumn("age_q", F.floor(df.age_corr/5) + 1)
    
    return df

def age_aberr(df):
    
    dict_age = {}

    dict_age["age < 15 et 'type_benef' = Actif "] = df.filter((df.age < 15) & (df.type_benef == "Actif")).count()
    dict_age["age > 70 et 'type_benef' = Actif "] = df.filter((df.age > 70) & (df.type_benef == "Actif")).count()
    dict_age["age < 16 et 'type_benef' = Veuf "] = df.filter((df.age < 16) & (df.type_benef == "Veuf")).count()
    dict_age["age > 110"] = df.filter(df.age > 110).count()

    return dict_age

def assiette_operations(df):

    partition = Window.partitionBy(colnames.TYPE_ADHERENT, colnames.SEXE)
    
    df = df.withColumn("assiettemoy", F.avg(F.when(
              (F.col(colnames.TYPE_BENEFICIAIRE) == "Adhérent") & 
              ( (F.col(colnames.ASSIETTE_COTISATION) > 0)  & (F.col(colnames.ASSIETTE_COTISATION).isNotNull()) )  , df.assiette_cotisation)).over(partition))
               
    df = df.withColumn(colnames.ASSIETTE_COTISATION, F.when(
              (F.col(colnames.TYPE_BENEFICIAIRE) == "Adhérent") & 
              ( (F.col(colnames.ASSIETTE_COTISATION) <= 0)  | (F.col(colnames.ASSIETTE_COTISATION).isNull()) ), F.col("assiettemoy"))
                                                      .otherwise(colnames.ASSIETTE_COTISATION))

    return  df

def assiette_aberr(df):
    
    nb_assiette_aberr = df.filter((F.col(colnames.TYPE_BENEFICIAIRE) == "Adhérent") & (F.col(colnames.ASSIETTE_COTISATION) <= 0)).count()

    assiette_aberr = {}
    assiette_aberr["assiette négative ou nulle"] = nb_assiette_aberr

    return assiette_aberr

def sexe_operations(df):

    df = df.withColumn("sexe_binaire", F.when(df.sexe == "M", True).when(df.sexe == "F", False) )
    
    df_sexe = df.filter(df.type_beneficiaire.isin(["Adhérent", "Conjoint"])).groupBy(colnames.ID_ADHERENT_A) \
                                                  .agg(
                                                  F.first(F.when(df.type_beneficiaire == "Adhérent", df.sexe_binaire), ignorenulls=True).alias("sexe_adherent"),
                                                  F.last(F.when(df.type_beneficiaire == "Conjoint", df.sexe_binaire), ignorenulls=True).alias("sexe_conjoint"),
                                                  F.count(F.when(df.type_beneficiaire == "Adhérent", F.col(colnames.ID_ADHERENT_A))).alias("N_adherent"),
                                                  F.count(F.when(df.type_beneficiaire == "Conjoint", F.col(colnames.ID_ADHERENT_A))).alias("N_conjoint"))

    df_sexe = df_sexe.withColumn("sexe_adherent", 
                                 F.when( (df_sexe.sexe_adherent.isNull()) & (df_sexe.N_conjoint > 0) & (df_sexe.N_adherent > 0), ~ df_sexe.sexe_conjoint )
                                   .otherwise(df_sexe.sexe_adherent))

    df_sexe = df_sexe.withColumn("sexe_conjoint", 
                                 F.when( (df_sexe.sexe_conjoint.isNull()) & (df_sexe.N_conjoint > 0) & (df_sexe.N_adherent > 0), ~ df_sexe.sexe_adherent )
                                   .otherwise(df_sexe.sexe_conjoint))
    
    #Jointure entre df et df_sexe
    df = df.join(df_sexe, ['id_adherent_a'], "full")
    
    #Remplissage des valeurs manquantes dans df
    df = df.withColumn("sexe_binaire", 
                                 F.when( (df.sexe_binaire.isNull()) & (df.type_beneficiaire == "Adhérent")  & (df.N_conjoint > 0), ~ df.sexe_conjoint )
                                                .when( (df.sexe_binaire.isNull()) & (df.type_beneficiaire == "Conjoint")  & (df.N_adherent > 0), ~ df.sexe_adherent )
                                                .otherwise(df.sexe_binaire))
    
    df = df.withColumn(colnames.SEXE, F.when(df.sexe_binaire == True, "M").when(df.sexe_binaire == False, "F").otherwise(colnames.SEXE))\
           .drop("N_adherent", "N_conjoint", "sexe_binaire", "sexe_adherent", "sexe_conjoint")

    return df

def sexe_aberr(df):
    
    df = df.withColumn("sexe_binaire", F.when(df.sexe == "M", True).when(df.sexe == "F", False) )

    
    df_sexe = df.filter(df.type_beneficiaire.isin(["Adhérent", "Conjoint"])).groupBy(colnames.ID_ADHERENT_A) \
                                                  .agg(
                                                  F.first(F.when(df.type_beneficiaire == "Adhérent", df.sexe_binaire), ignorenulls=True).alias("sexe_adherent"),
                                                  F.last(F.when(df.type_beneficiaire == "Conjoint", df.sexe_binaire), ignorenulls=True).alias("sexe_conjoint"),
                                                  F.count(F.when(df.type_beneficiaire == "Adhérent", F.col(colnames.ID_ADHERENT_A))).alias("N_adherent"),
                                                  F.count(F.when(df.type_beneficiaire == "Conjoint", F.col(colnames.ID_ADHERENT_A))).alias("N_conjoint"))

    df_sexe = df_sexe.withColumn("aberr", F.when( 
        ((df_sexe.sexe_adherent.isNull()) & (df_sexe.N_conjoint > 0) & (df_sexe.N_adherent > 0)) |
        ((df_sexe.sexe_conjoint.isNull()) & (df_sexe.N_conjoint > 0) & (df_sexe.N_adherent > 0)), 1).otherwise(0))
    
    nb_aberr = df_sexe.filter(df_sexe.aberr == 1).count()

    sexe_aberr = {}
    sexe_aberr["Sexe du conjoint/adhérent absent"] = nb_aberr

    return sexe_aberr

def operations_ald(df, annee : int):

    df = df.withColumn("is_ALD", F.when( (F.year(F.col(colnames.DATE_DEBUT_ACCORD_ALD)) <= annee) & 
                                         (F.year(F.col(colnames.DATE_FIN_ACCORD_ALD)) >= annee) , 1).otherwise(0))  
                                         
    df = df.groupBy(colnames.ID_BENEFICIAIRE_ALD).agg(F.max("is_ALD").alias("is_ALD"))

    return df

#Operations fusions
def fusion(df_1, df_2, cle_1 : str, cle_2 : str,  vars_requises : list, type_base : int, type_fusion : str = "left"):

    nb_manquants = dict.fromkeys(vars_requises)

    if (cle_1 not in df_1.columns) :
        text_error = f"{cle_1} n'est pas disponible dans la première base. "
        return text_error
    elif cle_2 not in df_2.columns :
        text_error = f"{cle_2} n'est pas disponible dans la deuxième base. "
        return text_error
    elif set(vars_requises).issubset(df_1.columns):
        for var in vars_requises:
            nb_manquants[var] = 0
    
    else:    

        vars_requises.append(cle_2)
        df_2 = df_2.select(vars_requises)
        nb_2 = df_2.count()

        if nb_2 == 0:
            text_error = "La deuxième base est vide"
            return text_error
        
        df_1 = df_1.join(df_2, df_1[cle_1]== df_2[cle_2], type_fusion)
        
        var = vars_requises[0]
        
        if type_base == 1: #DEMO + ALD

            nb_1 = df_1.filter(F.col(var).isNotNull()).count()

            for k in nb_manquants.keys():
                nb_manquants[k] = [nb_2 - nb_1, nb_2, f"{(round((nb_2 - nb_1)/nb_2, 3)*100)} %"]

        elif type_base == 2: #(CONSO/MEDIC) + (DEMO+ALD)

            df_nb_1 = df_1.filter(F.col(var).isNull()).groupBy(cle_1).count()
            nb_1 = df_nb_1.count() 

            for k in nb_manquants.keys():
                nb_manquants[k] = [nb_1, f"{round(nb_1/nb_2, 3)*100} %"]
        
        return df_1, nb_manquants

def traitement_ald_isnull(df):

    df = df.withColumn("is_ALD", F.when(F.col("is_ALD").isNull(), 0).otherwise(F.col("is_ALD")))
       
    return df

def manquants_mod_conso(df, colname : str, mods : dict):
    
    mod_list = list(mods.values())
    dict_proportions = dict.fromkeys(mod_list)
    df_ = df.filter(F.col(colname).isin(mod_list))
    tot = df_.count()
    N_manquants = df.count() - tot

    for mod in mod_list:
        nb_mod = df_.filter(F.col(colname) == mod).count()
        dict_proportions[mod] = nb_mod/tot
    
    mod_list_all = df.select(colname).distinct().rdd.flatMap(lambda x: x).collect()

    mod_list_replace = list(set(mod_list_all) - set(mod_list))

    for mod_r in mod_list_replace:
        
        df_mod = df.filter(F.col(colname) == mod_r)
        df_split = list(df_mod.randomSplit(list(dict_proportions.values())))
        for df_mod_r, mod in zip(df_split, dict_proportions.keys()):
            df_mod_r = df_mod_r.withColumn(colname, F.lit(mod))
            df_ = df_.union(df_mod_r)
    
    return df_, N_manquants

def convert_dates(df, dict_formats : dict):
    #Conversion des dates si toujours string
    
    L_schema = [ele.simpleString().split(":") for ele in df.schema]
    dict_schema = {ele[0] : ele[1] for ele in L_schema} #{"nom_colonne" : "type_colonne"}

    L_dates = list(dict_formats.keys())
    for str_date in L_dates:
        if dict_schema[str_date] != "date": #La date n'est pas convertie
            df = df.withColumn(str_date, F.to_date(F.to_timestamp(str_date, dict_formats[str_date])))
            
    return df

def conso_select(df, annee_inventaire : int):
    dict_dates = {k : v for k, v in coldict.dates_formats_dict.items() if k in df.columns}
    df = convert_dates(df, dict_dates)

    N = df.count()
    for k in dict_dates.keys():
        name = "annee_" + str(k)
        df = df.withColumn(name, F.year(F.col(k)))
    
    N_s = 0
    N_d = 0
    N_r = 0
    N_l = 0

    if colnames.DATE_DEBUT_SOIN in df.columns:
        df = df.filter( (F.col("annee_" + colnames.DATE_DEBUT_SOIN).isNotNull()) & (F.col("annee_" + colnames.DATE_DEBUT_SOIN) <= annee_inventaire))
        N_s = df.count()

    if colnames.DATE_RECEPTION in df.columns:
        df = df.filter( (F.col("annee_" + colnames.DATE_RECEPTION).isNotNull()) & (F.col("annee_" + colnames.DATE_RECEPTION) <= annee_inventaire))
        N_r = df.count()
        N_d = N_s - N_r
    
    if colnames.DATE_LIQUIDAITON in df.columns:
        df = df.filter( (F.col("annee_" + colnames.DATE_LIQUIDAITON).isNotNull()) & (F.col("annee_" + colnames.DATE_LIQUIDAITON) <= annee_inventaire))
        N_l = N_r - df.count()

   
    dict_tardifs = {"Nb_tardifs" : [N_d/N_s if N_s != 0 else N_d],
                        "Nb_tardifs_liquidation" : [N_l/N_s if N_s != 0 else N_l],
                        "Sinistres_annee" : N_s,
                        "Base_initiale" : N}
    
    return df, dict_tardifs

def traitement_conso(df, colname : str, coltype : str) :

    #Traitements prix unitaire
    dict_aberr = {}
    
    N = df.count()
    df = df.filter(F.col(colname).isNotNull())

    dict_aberr["Nb_manquants"] = N - df.count()
    
    if coltype in [coldict.SPARK_DOUBLE, coldict.SPARK_INT]:
        df = df.filter(F.col(colname) > 0)
        dict_aberr["Nb <= 0"] = N - df.count() - dict_aberr["Nb_manquants"]

    return df, dict_aberr

def calculs_conso(df, l_col : list = [colnames.COEFFICIENT, colnames.QUANTITE, colnames.TAUX_REMBOURSEMENT]):

    #Calculs quantite = quantite * coefficient
    if l_col[0] in df.columns:
        df = df.withColumn("quantite_calcule", F.when(df[l_col[0]].isNotNull(), df[l_col[1]] * df[l_col[0]]) \
                                                .otherwise(df[l_col[1]]))
    else:
        df = df.withColumn("quantite_calcule", df[l_col[1]])
    
    #Valeurs taux de remboursement et conversion en valeur réelle

    taux_moy = df.agg({l_col[2] : "mean"}).rdd.flatMap(lambda x: x).collect()[0]

    if taux_moy > 1:
        df = df.withColumn(l_col[2], F.col(l_col[2]) / 100)
    
    df = df.withColumn(l_col[2], F.when(F.col(l_col[2]) > 1, 1).otherwise(F.col(l_col[2])))

    return df



#D_aberr : {variable définie dans coldict : dictionnaire des aberrations(peut être la sortie d'une fonction)}
