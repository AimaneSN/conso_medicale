#importation des packages

import findspark
findspark.init()
import pyspark
import openpyxl
import os
import colnames

#import append_excel
from pyspark.sql import SparkSession, Row, HiveContext, Window
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from joblibspark import register_spark
import pyspark.pandas as ps
import unidecode #unicode to ASCII
import  pyspark.sql.functions  as fy

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re as re
import inspect
import sys
import gc
import datetime

import pyspark.ml as pyml

from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, Bucketizer
from pyspark.ml.stat import KolmogorovSmirnovTest, ChiSquareTest
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator, ClusteringEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans

from sklearn.linear_model import BayesianRidge, Ridge
from sklearn.model_selection import train_test_split
from sklearn.experimental import enable_iterative_imputer  
from sklearn.kernel_approximation import Nystroem
from sklearn.impute import SimpleImputer, IterativeImputer
from sklearn.pipeline import make_pipeline
import sklearn.ensemble as sk_e
from sklearn.model_selection import cross_val_score
from sklearn.neighbors import KNeighborsRegressor

#sp1.sparkContext.getConf().get("spark.executor.memory")
sp1 = SparkSession.builder.config("spark.driver.memory", "2g").appName('conso_medicale').enableHiveSupport().getOrCreate()
#conf=SparkConf()

#chemins des bases brutes
conso2019_path = "/home/aimane/Documents/BDDCNSS/conso2019.txt"
assiettes2019_path ="/home/aimane/Documents/BDDCNSS/assiettes_2019.txt"
ald_path = "/home/aimane/Documents/BDDCNSS/ALD_2015_2019.txt"
medic_path = "/home/aimane/Documents/BDDCNSS/médic_2019.txt"

#chemins des bases nettoyées
conso2019n_path = "/media/aimane/A376FE9926C3C949/Users/Aimane/Desktop/bases_csv/conso2019.csv"
assiettes2019n_path = "/media/aimane/A376FE9926C3C949/Users/Aimane/Desktop/bases_csv/assiettes2019.csv"
aldn_path = "/media/aimane/A376FE9926C3C949/Users/Aimane/Desktop/bases_csv/ALD2015_2019.csv"

base_full_path = "/home/aimane/Documents/BDDCNSS/base_full_join_.csv"
base_inner_path = "/home/aimane/Documents/BDDCNSS/base_inner_join_.csv"

def convert_column(df, column_name : str, coltype):
    # arguments: dataframe spark[df], nom de la colonne à convertir [column_name:string], type vers lequel faire la conversion
    #[coltype: DoubleType() ou DateType() ou StringType()]
    # output : dataframe de départ avec la colonne convertie
    if (coltype==DoubleType()):
        df=df.withColumn(column_name, fy.regexp_replace(column_name, ",", "."))
        df=df.withColumn(column_name, df[column_name].cast(DoubleType()))
    elif (coltype==DateType()):
        df=df.withColumn(column_name, df[column_name].cast(StringType()))
        df=df.withColumn(column_name,fy.to_date(fy.to_timestamp(df[column_name], "yyyyMMdd")))
    else:
        df=df.withColumn(column_name, df[column_name].cast(coltype))
    return df

#Détection du schéma

def get_file(filepath : str, filesep : str):
    '''Argument : filepath: chemin du fichier à importer
       Output : Si le programme réussit à deviner le schéma : DataFrame Spark du fichier importé (df)
                Si échec du programme : affichage d'un message d'erreur et retour de la valeur -1 '''
    try:
        df=sp1.read.options(inferSchema=True).option("encoding", "ISO-8859-1").csv(filepath, header=True, sep=filesep)
        print("schéma trouvé")
        return df
    except:
        print("Erreur, le programme ne peut pas trouver le schéma")
        return -1

#Importation des bases de données

base_conso = get_file(conso2019_path, ";")
base_assiette = get_file(assiettes2019_path, ";")
base_ald = get_file(ald_path, ";")
base_medic = get_file(medic_path, ";")

base_full = get_file(base_full_path, ",")
base_inner = get_file(base_inner_path, ",")

#basen_conso=get_schema(conso2019n_path)
#basen_assiette=get_schema(assiettes2019n_path)
#basen_ald=get_schema(aldn_path)


#Définition des noms des colonnes et leurs indices respectifs

assiette_indices = {colnames.ID_BENEFICIAIRE_A : 0,
               colnames.ID_ADHERENT_A : 1,
               colnames.TYPE_ADHERENT : 3,
               colnames.TYPE_BENEFICIAIRE : 4,
               colnames.DNAISSANCE : 5,
               colnames.SEXE : 6,
               colnames.ASSIETTE_COTISATION : 8,
               colnames.DATE_FIN_DO : 9,
               colnames.VILLE : 10,
               colnames.NOMBRE_MOIS : 12} 

conso_indices = {colnames.ID_BENEFICIAIRE_C : 1,
            colnames.ID_ADHERENT_C : 0,
            colnames.RANG : 2,
            colnames.DATE_DEBUT_SOIN : 5, 
            colnames.CODE_ACTE : 9,
            colnames.LIBELLE_ACTE : 10,
            colnames.PRIX_UNITAIRE : 13}

ald_indices = {colnames.ID_ADHERENT_ALD : 0,
          colnames.ID_BENEFICIAIRE_ALD : 1,
          colnames.RANG_ALD : 2,
          colnames.CODE_ALD_ALC : 3,
          colnames.DATE_DEBUT_ACCORD_ALD : 4,
          colnames.DATE_FIN_ACCORD_ALD : 5}

#Renommage des colonnes avec les définitions ci-dessus

for indice_colonne in assiette_indices.values():
    base_assiette = base_assiette.withColumnRenamed(base_assiette.columns[indice_colonne], list(assiette_indices.keys())[list(assiette_indices.values()).index(indice_colonne)])

for indice_colonne in conso_indices.values():
    base_conso = base_conso.withColumnRenamed(base_conso.columns[indice_colonne], list(conso_indices.keys())[list(conso_indices.values()).index(indice_colonne)])

for indice_colonne in ald_indices.values():
    base_ald = base_ald.withColumnRenamed(base_ald.columns[indice_colonne], list(ald_indices.keys())[list(ald_indices.values()).index(indice_colonne)])

#Conversion vers les types appropriés

base_assiette = base_assiette.withColumn(colnames.DNAISSANCE, fy.to_timestamp(colnames.DNAISSANCE, "yyyy.MM.dd HH:mm:ss"))
base_assiette = base_assiette.withColumn(colnames.DATE_FIN_DO, fy.when(fy.length(fy.col(colnames.DATE_FIN_DO))>1,fy.to_timestamp(colnames.DATE_FIN_DO, "yyyyMMdd")).otherwise(fy.col(colnames.DATE_FIN_DO))) #date_fin_DO contient des observations égales à 0
base_assiette = convert_column(base_assiette, colnames.ASSIETTE_COTISATION, DoubleType())
base_assiette = convert_column(base_assiette, colnames.ID_BENEFICIAIRE_A, StringType())

base_conso = convert_column(base_conso, colnames.MONTANT_REMBOURSE, DoubleType())
base_conso = convert_column(base_conso, colnames.PRIX_UNITAIRE, DoubleType())
base_conso = convert_column(base_conso, colnames.DATE_DEBUT_SOIN, DateType())

base_ald = convert_column(base_ald, colnames.DATE_DEBUT_ACCORD_ALD, DateType())
base_ald = convert_column(base_ald, colnames.DATE_FIN_ACCORD_ALD, DateType())

#Création des variables AGE et TRANCHE_AGE (dans la base assiettes)

base_assiette = base_assiette.withColumn("age", fy.round(fy.months_between(fy.lit(datetime.datetime(2019,12,31)),
                                                        fy.col(colnames.DNAISSANCE))/fy.lit(12), 2))\
                           .withColumn("age", fy.when(fy.col("age")>110,110).otherwise(fy.col("age"))) 

base_assiette = base_assiette.selectExpr("*",
"CASE "
"WHEN age < 5 THEN \"[0-5[\" "
"WHEN age BETWEEN 5 AND 10 THEN \"[5-10[\" "
"WHEN age BETWEEN 10 AND 15 THEN \"[10-15[\" "
"WHEN age BETWEEN 15 AND 20 THEN \"[15-20[\" "
"WHEN age BETWEEN 20 AND 25 THEN \"[20-25[\" "
"WHEN age BETWEEN 25 AND 30 THEN \"[25-30[\" "
"WHEN age BETWEEN 30 AND 35 THEN \"[30-35[\" "
"WHEN age BETWEEN 35 AND 40 THEN \"[35-40[\" "
"WHEN age BETWEEN 40 AND 45 THEN \"[40-45[\" "
"WHEN age BETWEEN 45 AND 50 THEN \"[45-50[\" "
"WHEN age BETWEEN 50 AND 55 THEN \"[50-55[\" "
"WHEN age BETWEEN 55 AND 60 THEN \"[55-60[\" "
"WHEN age BETWEEN 60 AND 65 THEN \"[60-65[\" "
"WHEN age BETWEEN 65 AND 70 THEN \"[65-70[\" "
"WHEN age BETWEEN 70 AND 75 THEN \"[70-75[\" "
"WHEN age BETWEEN 75 AND 80 THEN \"[75-80[\" "
"WHEN age BETWEEN 80 AND 85 THEN \"[80-85[\" "
"WHEN age BETWEEN 85 AND 90 THEN \"[85-90[\" "
"WHEN age BETWEEN 90 AND 95 THEN \"[90-95[\" "
"WHEN age BETWEEN 95 AND 100 THEN \"[95-100[\" "
"WHEN age BETWEEN 100 AND 105 THEN \"[100-105[\" "
"WHEN age BETWEEN 105 AND 110 THEN \"[105-110[\" "
"WHEN age>110 THEN 110 "

"ELSE \" VALEUR AGE INVALIDE \" "

"END "
"AS tranche")

base_assiette = base_assiette.withColumn("type_benef", fy.when(fy.col(colnames.TYPE_BENEFICIAIRE)=="Adhérent", fy.col("type_adherent"))
                                                         .otherwise(fy.col(colnames.TYPE_BENEFICIAIRE)))


#traitement âges
# agep=Window.partitionBy("type_adherent", "type_beneficiaire", "SEXE")
# base_assiette3=base_assiette2.withColumn("age_moyen",fy.mean("age").over(agep))  #lent

# sp1.sql("SELECT * FROM assiette_ WHERE age < 15 AND type_benef = \"Actif\"")

# base_assiette3=base_assiette3.withColumn("age_corr", fy.when((fy.col("age") < 15) & (fy.col("type_benef")=="Actif"), 15) \
#                                                        .when((fy.col("age") > 65) & (fy.col("type_benef")=="Actif"), fy.col("age_moyen")) \
#                                                        .when((fy.col("age") < 16) & (fy.col("type_benef")=="Veuf"), 16) \
#                                                        .when(fy.col("age") > 110, fy.col("age_moyen")) \
#                                                        .otherwise(fy.col("age")))

winpartition = Window.partitionBy("id_adherent_a").orderBy("type_beneficiaire")
benef_partition = Window.partitionBy("id_beneficiaire_a").orderBy("type_beneficiaire")
benefc_partition = Window.partitionBy("id_beneficiaire_c")

#nb d'actes par bénéficiaire (base conso)
base_conso = base_conso.withColumn("nb_actes", fy.when(fy.col(colnames.ID_BENEFICIAIRE_C).isNotNull(), fy.count(colnames.ID_BENEFICIAIRE_C).over(benefc_partition)))

#annualisation des assiettes de cotisation

base_assiette = base_assiette.withColumn("assiette_cotisation_annualisee", fy.when( (fy.col(colnames.NOMBRE_MOIS).isNotNull()) & (fy.col(colnames.NOMBRE_MOIS)!=0), \
                                          fy.col(colnames.ASSIETTE_COTISATION) * 12 / fy.col(colnames.NOMBRE_MOIS))
                                          .otherwise(0))

#Echantillons des bases initiales. Pour utiliser les bases complètes, il suffit d'effacer la partie ".limit(80000)" dans les affectations ci-après#
assiette_e = base_assiette.limit(85000)
conso_e = base_conso.limit(85000)
ald_e = base_ald.limit(85000)

assiette_conso_e = conso_e.join(assiette_e, conso_e.id_beneficiaire_c==assiette_e.id_beneficiaire_a, "full")
assiette_conso_ald_e = assiette_conso_e.join(ald_e, assiette_conso_e.id_beneficiaire_a==ald_e.id_beneficiaire_ald, "full")

assiette_conso_i = conso_e.join(assiette_e, conso_e.id_beneficiaire_c==assiette_e.id_beneficiaire_a, "inner")
assiette_conso_ald_i = assiette_conso_i.join(ald_e, assiette_conso_i.id_beneficiaire_a==ald_e.id_beneficiaire_ald, "full")

assiette_conso_ald_e = assiette_conso_ald_e.withColumn("ALD_n", fy.when(((fy.col("date_fin_accord_ald")>=datetime.datetime(2019,1,1)) & (fy.col("date_debut_soin").isNull())) 
                                    |((fy.col("date_debut_soin").isNotNull()) & (fy.col("date_debut_soin").between(fy.col("date_debut_accord_ald"),fy.col("date_fin_accord_ald"))) )
                                                                                                                                                            
, 1).otherwise(0))                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                
assiette_conso_ald_e = assiette_conso_ald_e.withColumn("id_beneficiaire_a", fy.when(fy.col("id_beneficiaire_a").isNull() & fy.col("id_beneficiaire_c").isNotNull(), fy.col("id_beneficiaire_c")).when(fy.col("id_beneficiaire_a").isNull() & fy.col("id_beneficiaire_ald").isNotNull(),
fy.col("id_beneficiaire_ald")).otherwise(fy.col("id_beneficiaire_a")))

assiette_conso_ald_e.coalesce(1).write.csv('base_full_join.csv', header='true')

#Attribution du SEXE 'F' aux bénéficiaires de RANG=3,...,10
jffp=assiette_conso_ald_e.withColumn("SEXE", fy.when( assiette_conso_ald_e["RANG"].between(3,10), "F").otherwise(assiette_conso_ald_e["SEXE"]))

#Attribution aux conjoints(RANG=2) le sexe opposé de l'adhérent (quand il est disponible) 
jffp=jffp.withColumn("SEXE_ad", fy.first(assiette_conso_ald_e["SEXE"]).over(winpartition))
jffp=jffp.withColumn("SEXE", fy.when( (assiette_conso_ald_e["RANG"]==2) & (assiette_conso_ald_e["SEXE"].isNull()) & (fy.first(assiette_conso_ald_e["SEXE"]).over(winpartition).isNotNull()), fy.first(assiette_conso_ald_e["SEXE"]).over(winpartition)).otherwise(assiette_conso_ald_e["SEXE"]))

assiette_conso_ald_e.createOrReplaceTempView("base")

nb_actes=sp1.sql("SELECT sexe, ALD_n, tranche, code_acte, COUNT(libelle_acte) AS NOMBRE_ACTES, COUNT (DISTINCT id_beneficiaire_a) AS EFFECTIF FROM base WHERE sexe IS NOT NULL AND tranche IS NOT NULL AND libelle_acte IS NOT NULL "
                  "GROUP BY sexe, tranche, ALD_n, code_acte "
                  "ORDER BY sexe, ALD_n,CASE "
                  "WHEN tranche=\"[0-5[\" THEN 1 "
                  "WHEN tranche=\"[5-10[\" THEN 2 "
                  "WHEN tranche=\"[10-15[\" THEN 3 "
                  "WHEN tranche=\"[15-20[\" THEN 4 "
                  "WHEN tranche=\"[20-25[\" THEN 5 "
                  "WHEN tranche=\"[25-30[\" THEN 6 "
                  "WHEN tranche=\"[30-35[\" THEN 7 "
                  "WHEN tranche=\"[35-40[\" THEN 8 "
                  "WHEN tranche=\"[40-45[\" THEN 9 "
                  "WHEN tranche=\"[45-50[\" THEN 10 "
                  "WHEN tranche=\"[50-55[\" THEN 11 "
                  "WHEN tranche=\"[55-60[\" THEN 12 "
                  "WHEN tranche=\"[60-65[\" THEN 13 "
                  "WHEN tranche=\"[65-70[\" THEN 14 "
                  "WHEN tranche=\"[70-75[\" THEN 15 "
                  "WHEN tranche=\"[75-80[\" THEN 16 "
                  "WHEN tranche=\"[80-85[\" THEN 17 "
                  "WHEN tranche=\"[85-90[\" THEN 18 "
                  "WHEN tranche=\"[90-95[\" THEN 19 "
                  "WHEN tranche=\"[95-100[\" THEN 20 "
                  "WHEN tranche=\"[100-105[\" THEN 21 "
                  "WHEN tranche=\"[105-110[\" THEN 22 "
                  "WHEN tranche=110 THEN 23 "
                  "END, CODE_ACTE")

effectifs = sp1.sql("SELECT sexe, ALD_n, tranche, COUNT (DISTINCT id_beneficiaire_a) AS EFFECTIF FROM base WHERE SEXE IS NOT NULL AND tranche IS NOT NULL "
                  "GROUP BY SEXE, tranche, ALD_n "
                  "ORDER BY SEXE, ALD_n,CASE "
                  "WHEN tranche=\"[0-5[\" THEN 1 "
                  "WHEN tranche=\"[5-10[\" THEN 2 "
                  "WHEN tranche=\"[10-15[\" THEN 3 "
                  "WHEN tranche=\"[15-20[\" THEN 4 "
                  "WHEN tranche=\"[20-25[\" THEN 5 "
                  "WHEN tranche=\"[25-30[\" THEN 6 "
                  "WHEN tranche=\"[30-35[\" THEN 7 "
                  "WHEN tranche=\"[35-40[\" THEN 8 "
                  "WHEN tranche=\"[40-45[\" THEN 9 "
                  "WHEN tranche=\"[45-50[\" THEN 10 "
                  "WHEN tranche=\"[50-55[\" THEN 11 "
                  "WHEN tranche=\"[55-60[\" THEN 12 "
                  "WHEN tranche=\"[60-65[\" THEN 13 "
                  "WHEN tranche=\"[65-70[\" THEN 14 "
                  "WHEN tranche=\"[70-75[\" THEN 15 "
                  "WHEN tranche=\"[75-80[\" THEN 16 "
                  "WHEN tranche=\"[80-85[\" THEN 17 "
                  "WHEN tranche=\"[85-90[\" THEN 18 "
                  "WHEN tranche=\"[90-95[\" THEN 19 "
                  "WHEN tranche=\"[95-100[\" THEN 20 "
                  "WHEN tranche=\"[100-105[\" THEN 21 "
                  "WHEN tranche=\"[105-110[\" THEN 22 "
                  "WHEN tranche=110 THEN 23 "
                  "END") 

#freq = freq.withColumn("fc", fy.round(freq["NOMBRE_ACTES"]/freq["EFFECTIF"], 3))

#calcul de la différence entre l'âge de l'adhérent et celui du conjoint, enfant,...
assiette_e=assiette_e.withColumn("diff_age", fy.months_between(fy.col("dnaissance"),fy.first("dnaissance").over(winpartition))/12)

#attribution de la date d'immatriculation de l'adhérent
assiette_e2=assiette_e.withColumn("imm_adherent", fy.first("DATE_IMMATRICULATION").over(winpartition))
#assiette_e2=assiette_e.withColumn("DATE_IMMATRICULATION", fy.when())

jfinner2.coalesce(1).write.csv('jfinner.csv', header='true')


#Traitement de la variable ASSIETTE_COTISATION
base_assiette=base_assiette.withColumn("assiette_adherent", fy.first("assiette_cotisation").over*(winpartition))

base_assiette=base_assiette.withColumn("assiette_corr", fy.when(fy.col("assiette_cotisation")==0, fy.col("assiette_adherent"))
                                             .otherwise(fy.col("assiette_cotisation")))

#######################################################################

####AGE ~ nb_actes + ALD_n (ou ald_alc ou code_garantie) + LIB_ACTE + prix_unitaire
####SEXE ~ nb_actes + LIB_ACTE 
bk=Bucketizer(splits=np.arange(0,115,5), inputCol="age", outputCol="tranche_age")

p=Window.partitionBy("id_beneficiaire_a")
d1 = base_inner.filter( (fy.col("rang")==1) ).select("id_beneficiaire_a", "SEXE", "age", "nb_actes", "ALD_n", "prix_unitaire")\
               .withColumn("prix_unitaire", fy.sum(fy.col("prix_unitaire")).over(p))\
               .distinct()

d2=bk.setHandleInvalid("keep").transform(d1)

#Il faut que les classes soient équilibrées
classes=d2.groupBy("tranche_age").count().toPandas()
tranches=list(classes["tranche_age"])
d3= sp1.createDataFrame([], schema=d2.schema)

for tr in tranches:
    d3=d3.union(d2.filter(fy.col("tranche_age")==tr).limit(1200))

rf_assembler=VectorAssembler(inputCols=["nb_actes", "ALD_n", "prix_unitaire"], outputCol="features")
d3=rf_assembler.transform(d3)

##RF:

rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'tranche_age')

rfModel = rf.fit(train)
predictions = rfModel.transform(test)

evaluator = MulticlassClassificationEvaluator(labelCol="tranche_age", predictionCol="prediction")
accuracy = evaluator.evaluate(predictions)
print("Accuracy = %s" % (accuracy))
print("Test Error = %s" % (1.0 - accuracy))

#train.cache()
#####
nb_max_arbre=20
for ntree in range(1, nb_max_arbre):
    rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'tranche_age', numTrees=ntree)
    rfModel = rf.fit(train)
    predictions = rfModel.transform(test)
    evaluator = MulticlassClassificationEvaluator(labelCol="tranche_age", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    error=1-accuracy
    print("%s arbre => Accuracy %g, Error = %g" % (ntree,accuracy,error))
##LR:

lr = LinearRegression(featuresCol = 'features', labelCol='age', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train)
print("Les coefficients sont: " + str(lr_model.coefficients))
print("La constante est : " + str(lr_model.intercept))

qualite = lr_model.summary
print("RMSE: %f" % qualite.rootMeanSquaredError)
print("r2: %f" % qualite.r2)

#Logistic:

sx_indexer = StringIndexer().setInputCol("SEXE").setOutputCol("SEXE_label")
lb_indexer = StringIndexer().setInputCol("libelle_acte").setOutputCol("libelle_id")

df2=sx_indexer.fit(train).transform(train)
df2=lb_indexer.fit(df2).transform(df2)

rf_assembler=VectorAssembler(inputCols=["nb_actes", "libelle_id"], outputCol="features")
df2=rf_assembler.transform(df2.limit(400000))

lr = LogisticRegression(featuresCol = 'features', labelCol = 'SEXE_label', maxIter=10)
lrModel = lr.fit(df2)

beta = np.sort(lrModel.coefficients)
plt.plot(beta)
plt.ylabel('Coefficients beta du modele:')
plt.show()

qualite = lrModel.summary
roc = qualite.roc.toPandas()
plt.plot(roc['FPR'],roc['TPR'])
plt.ylabel('Taux de vrais positifs')
plt.xlabel('Taux de faux positifs')
plt.title('Courbe ROC')
plt.show()
print('Valeur ROC: ' + str(qualite.areaUnderROC))
