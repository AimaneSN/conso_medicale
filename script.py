#importation des packages
import pyspark
from pyspark.sql import SparkSession, Row, HiveContext, Window
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

import  pyspark.sql.functions  as fy
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import re as re
#from pandas_profiling import ProfileReport
import inspect
import sys
import gc
import datetime

import pyspark.ml as pyml

from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.stat import KolmogorovSmirnovTest, ChiSquareTest
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

sp1 = SparkSession.builder.appName('myApp').enableHiveSupport().getOrCreate()

#chemins des bases de données brutes
conso2019_path = "/home/aimane/Documents/BDDCNSS/conso2019.txt"
assiettes2019_path ="/home/aimane/Documents/BDDCNSS/assiettes_2019.txt"
ald_path = "/home/aimane/Documents/BDDCNSS/ALD_2015_2019.txt"
medic_path = "/home/aimane/Documents/BDDCNSS/médic_2019.txt"

#chemins des bases nettoyées
conso2019n_path = "/media/aimane/A376FE9926C3C949/Users/Aimane/Desktop/bases_csv/conso2019.csv"
assiettes2019n_path = "/media/aimane/A376FE9926C3C949/Users/Aimane/Desktop/bases_csv/assiettes2019.csv"
aldn_path = "/media/aimane/A376FE9926C3C949/Users/Aimane/Desktop/bases_csv/ALD2015_2019.csv"


def convert_column(df, column_name, coltype):
    ''' Arguments: dataframe spark[df], nom de la colonne à convertir [column_name:string], type vers lequel faire la conversion
    [coltype: DoubleType() ou DateType() ou StringType()]
        Output : dataframe de départ avec la colonne convertie '''
    if (coltype==DoubleType()):
        df=df.withColumn(column_name, fy.regexp_replace(column_name, ",", "."))
        df=df.withColumn(column_name, df[column_name].cast(DoubleType()))
    elif (coltype==DateType()):
        df=df.withColumn(column_name, df[column_name].cast(StringType()))
        df=df.withColumn(column_name,fy.to_date(fy.to_timestamp(df[column_name], "yyyyMMdd")))
    else:
        df=df.withColumn(column_name, df[column_name].cast(coltype))
    return df

def get_file(filepath):
    '''Argument : filepath: chemin du fichier à importer
       Output : Si le programme réussit à deviner le schéma : DataFrame Spark du fichier importé (df)
                Si échec du programme : affichage d'un message d'erreur et retour de la valeur -1 '''
    try:
        df=sp1.read.options(inferSchema=True).option("encoding", "ISO-8859-1").csv(filepath, header=True, sep=";")
        print("schéma trouvé")
        return df
    except:
        print("Erreur, le programme ne peut pas trouver le schéma")
        return -1
        

###Importation des bases (conso2019 et assiettes2019)

base_conso=get_file(conso2019_path)
base_assiette=get_file(assiettes2019_path)
base_ald=get_file(ald_path)
base_medic=get_file(medic_path)

#basen_conso=get_schema(conso2019n_path)
#basen_assiette=get_schema(assiettes2019n_path)
#basen_ald=get_schema(aldn_path)

base_assiette= base_assiette.withColumnRenamed(base_assiette.columns[0], "beneficiaire") \
                            .withColumnRenamed(base_assiette.columns[1], "adherent") \
                            .withColumnRenamed(base_assiette.columns[3], "type_adherent") \
                            .withColumnRenamed(base_assiette.columns[4], "type_beneficiaire") \
                            .withColumnRenamed(base_assiette.columns[5], "dnaissance")
base_conso= base_conso.withColumnRenamed(base_conso.columns[0], "adherent") \
                      .withColumnRenamed(base_conso.columns[1], "beneficiairec") \
                      .withColumnRenamed(base_conso.columns[3], "numdossier")
base_ald = base_ald.withColumnRenamed(base_ald.columns[0], "adherent") \
                   .withColumnRenamed(base_ald.columns[1], "beneficiaire") \
                   .withColumnRenamed(base_ald.columns[4], "date_debut_accord")

base_assiette=base_assiette.withColumn("dnaissance", fy.to_timestamp("dnaissance", "yyyy.MM.dd HH:mm:ss"))
base_assiette=base_assiette.withColumn("DATE_FIN_DO", fy.when(fy.length(fy.col("DATE_FIN_DO"))>1,fy.to_timestamp("DATE_FIN_DO", "yyyyMMdd")).otherwise(fy.col("DATE_FIN_DO"))) #date_fin_DO contient des observations égales à 0

#base_assiette=base_assiette.withColumn("DATE_IMMATRICULATION", fy.to_timestamp("DATE_IMMATRICULATION", "yyyy-MM-dd"))
base_assiette=convert_column(base_assiette, "ASSIETTE_COTISATION", DoubleType())
base_assiette=convert_column(base_assiette, "beneficiaire", StringType())
base_assiette=convert_column(base_assiette, "Employeur", StringType())

base_conso=convert_column(base_conso, "MONTANT_REMBOURSE", DoubleType())
base_conso=convert_column(base_conso, "PRIX_UNITAIRE", DoubleType())
base_conso=convert_column(base_conso, "COEFFICIENT", DoubleType())
base_conso=convert_column(base_conso, "DATE_ARRIVE", DateType())
base_conso=convert_column(base_conso, "DATE_DEBUT_SOIN", DateType())
base_conso=convert_column(base_conso, "DATE_SAISIE", DateType())
base_conso=convert_column(base_conso, "DATE_PAIEMENT", DateType())
base_ald=convert_column(base_ald, "date_debut_accord", DateType())
base_ald=convert_column(base_ald, "date_fin_accord", DateType())

#tranches d'âge (base assiettes)
base_assiette2=base_assiette.withColumn("age", fy.round(fy.months_between(fy.lit(datetime.datetime(2019,12,31)),
                                                        fy.col("dnaissance"))/fy.lit(12), 2))
base_assiette2.createOrReplaceTempView("assiette_")

base_assiette2= sp1.sql("SELECT *, "
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
"AS tranche FROM assiette_")

base_assiette2=base_assiette2.withColumn("type_benef", fy.when(fy.col("type_beneficiaire")=="Adhérent", fy.col("type_adherent"))
                                                         .otherwise(fy.col("type_beneficiaire")))

base_assiette2.createOrReplaceTempView("assiette_")

#traitement âges
# agep=Window.partitionBy("type_adherent", "type_beneficiaire", "SEXE")
# base_assiette3=base_assiette2.withColumn("age_moyen",fy.mean("age").over(agep))  #lent

# sp1.sql("SELECT * FROM assiette_ WHERE age < 15 AND type_benef = \"Actif\"")

# base_assiette3=base_assiette3.withColumn("age_corr", fy.when((fy.col("age") < 15) & (fy.col("type_benef")=="Actif"), 15) \
#                                                        .when((fy.col("age") > 65) & (fy.col("type_benef")=="Actif"), fy.col("age_moyen")) \
#                                                        .when((fy.col("age") < 16) & (fy.col("type_benef")=="Veuf"), 16) \
#                                                        .when(fy.col("age") > 110, fy.col("age_moyen")) \
#                                                        .otherwise(fy.col("age")))

winpartition=Window.partitionBy("adherent_a").orderBy("type_beneficiaire")
benef_partition=Window.partitionBy("beneficiaire_a").orderBy("type_beneficiaire")

#base_assiette3=base_assiette3.withColumn("diff_age", fy.first("age_corr").over(winpartition) - fy.col("age_corr"))

base_assiette2=base_assiette2.withColumnRenamed(base_assiette2.columns[0], "beneficiaire_a") \
              .withColumnRenamed(base_assiette2.columns[1], "adherent_a")
base_conso=base_conso.withColumnRenamed(base_conso.columns[0], "adherentc")

#annualisation des assiettes de cotisation

base_assiette2=base_assiette2.withColumn("assiette_cotisation2", fy.when( (fy.col("NOMBRE_MOIS").isNotNull()) & (fy.col("NOMBRE_MOIS")!=0), \
                                          fy.col("ASSIETTE_COTISATION") * 12 / fy.col("NOMBRE_MOIS"))
                                          .otherwise(0))


#Echantillons des bases initiales. Pour utiliser les bases complètes, il suffit d'effacer la partie ".limit(80000)" dans les affectations ci-après

assiette_e=base_assiette2.limit(80000)
conso_e=base_conso.limit(80000)
ald_e=base_ald.limit(80000)

j1 = conso_e.join(assiette_e, conso_e.beneficiairec==assiette_e.beneficiaire_a, "full")
ald_e=ald_e.withColumnRenamed("rang", "rangg")
j2 = j1.join(ald_e, j1.beneficiaire_a==ald_e.beneficiaire, "full")

jff = j2.withColumn("ALD_n", fy.when(((fy.col("date_fin_accord")>=datetime.datetime(2019,1,1)) &                         (fy.col("DATE_DEBUT_SOIN").isNull())) 
                                    |((fy.col("DATE_DEBUT_SOIN").isNotNull()) & (fy.col("DATE_DEBUT_SOIN").between(fy.col("date_debut_accord"),fy.col("date_fin_accord"))) )
                                                                                                                                                            
, 1).otherwise(0))                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                
jff=jff.withColumn("beneficiaire_a", fy.when(fy.col("beneficiaire_a").isNull() & fy.col("beneficiairec").isNotNull(), fy.col("beneficiairec")).when(fy.col("beneficiaire_a").isNull() & fy.col("beneficiaire").isNotNull(),
fy.col("beneficiaire")).otherwise(fy.col("beneficiaire_a")))
                                  
jffinner= jff.filter( (fy.col("DATE_IMMATRICULATION").isNotNull()) & (fy.col("RANG").isNotNull()) ) #95865
jfinner2=jffinner.dropDuplicates() #95067
jfinner2.createOrReplaceTempView("jffinner")


#Attribution du SEXE 'F' aux bénéficiaires de RANG=3,...,10
jffp=jff.withColumn("SEXE", fy.when( jff["RANG"].between(3,10), "F").otherwise(jff["SEXE"]))

#Attribution aux conjoints(RANG=2) le sexe opposé de l'adhérent (quand il est disponible) 
jffp=jffp.withColumn("SEXE_ad", fy.first(jff["SEXE"]).over(winpartition))
jffp=jffp.withColumn("SEXE", fy.when( (jff["RANG"]==2) & (jff["SEXE"].isNull()) & (fy.first(jff["SEXE"]).over(winpartition).isNotNull()), fy.first(jff["SEXE"]).over(winpartition)).otherwise(jff["SEXE"]))


##Calcul des fréquences de consommation

nb_actes_=sp1.sql("SELECT COUNT(LIB_ACTE), SEXE, ALD_n, tranche FROM jffinner WHERE LIB_ACTE IS NOT NULL GROUP BY SEXE, tranche, ALD_n ORDER BY tranche")

effectifs_=sp1.sql("SELECT COUNT (DISTINCT beneficiaire_a), SEXE, ALD_n, tranche FROM jf WHERE beneficiaire_a IS NOT NULL GROUP BY SEXE, tranche, ALD_n") #effectifs des bénéficiaires dans la base démographique (assiettes)


##Autres traitements:

#calcul de la différence entre l'âge de l'adhérent et celui du conjoint, enfant,...
assiette_e=assiette_e.withColumn("diff_age", fy.months_between(fy.col("dnaissance"),fy.first("dnaissance").over(winpartition))/12)

#attribution de la date d'immatriculation de l'adhérent
assiette_e2=assiette_e.withColumn("imm_adherent", fy.first("DATE_IMMATRICULATION").over(winpartition))
#assiette_e2=assiette_e.withColumn("DATE_IMMATRICULATION", fy.when())

#nombre d'actes par bénéficiaire
actes=jfinner2.withColumn("nb_actes", fy.when(fy.col("beneficiairec").isNotNull(), fy.count("beneficiairec").over(benef_partition)))


#Attribution de l'assiette de cotisation de l'adhérent (si disponible) aux observations manquantes 
base_assiette2=base_assiette2.withColumn("assiette_adherent", fy.first("ASSIETTE_COTISATION").over(winpartition))

base_assiette2=base_assiette2.withColumn("assiette_corr", fy.when(fy.col("ASSIETTE_COTISATION")==0, fy.col("assiette_adherent"))
                                             .otherwise(fy.col("ASSIETTE_COTISATION")))

base_assiette2.select("ASSIETTE_COTISATION", "beneficiaire_a", "type_beneficiaire", "dnaissance").where(fy.col("ASSIETTE_COTISATION") < 0).show()


#nombre d'observations null par colonne
for col in jff.columns:
    print("La colonne " + col + " contient " + str(jff.filter(jff[col].isNull()).count())+ " observations manquantes\n")

##Exemple régression logistique (SEXE ~ TRANCHE_ASSIETTE + ALD_n 

#On travaille avec la base jfinner (jointure INNER JOIN) qui ne contient pas d'observations manquantes
jfinner3=jfinner2.drop("PRESCRIPTEUR", "Employeur", "numdossier", "secteur_soins", "mode_paiement")

#On choisit uniquement les adhérents pour cette régression, étant donné que l'assiette de cotisation est la même pour les autres membres du même ménage.
jfinner4=jfinner3.filter(fy.col("type_beneficiaire")=="Adhérent")

t1=jfinner4.selectExpr("SEXE", "ALD_n",
"CASE "
"WHEN ASSIETTE_COTISATION = 0  THEN \"0\" "
"WHEN ASSIETTE_COTISATION < 5000 AND ASSIETTE_COTISATION > 0  THEN \"]0-5000[\" "
"WHEN ASSIETTE_COTISATION BETWEEN 5000 AND 10000 THEN \"[5000-10000[\" "
"WHEN ASSIETTE_COTISATION BETWEEN 10000 AND 15000 THEN \"[10000-15000[\" "
"WHEN ASSIETTE_COTISATION BETWEEN 15000 AND 20000 THEN \"[15000-20000[\" "
"WHEN ASSIETTE_COTISATION BETWEEN 20000 AND 25000 THEN \"[20000-25000[\" "
"WHEN ASSIETTE_COTISATION BETWEEN 25000 AND 30000 THEN \"[25000-30000[\" "
"WHEN ASSIETTE_COTISATION BETWEEN 30000 AND 35000 THEN \"[30000-35000[\" "
"WHEN ASSIETTE_COTISATION BETWEEN 35000 AND 40000 THEN \"[35000-40000[\" "
"WHEN ASSIETTE_COTISATION BETWEEN 40000 AND 45000 THEN \"[40000-45000[\" "
"WHEN ASSIETTE_COTISATION BETWEEN 45000 AND 50000 THEN \"[45000-50000[\" "
"WHEN ASSIETTE_COTISATION BETWEEN 50000 AND 55000 THEN \"[50000-55000[\" "
"WHEN ASSIETTE_COTISATION BETWEEN 55000 AND 60000 THEN \"[55000-60000[\" "
"WHEN ASSIETTE_COTISATION BETWEEN 60000 AND 65000 THEN \"[60000-65000[\" "
"WHEN ASSIETTE_COTISATION BETWEEN 65000 AND 70000 THEN \"[65000-70000[\" "
"WHEN ASSIETTE_COTISATION>70000 THEN \"70000+\" "

"ELSE \" VALEUR ASSIETTE_COTISATION INVALIDE \" "

"END "
"AS tranche_assiette")

t1=t1.limit(5000)
cols=t1.columns

#Recodage des variables catégoriques ALD/nonALD et tranche_assiette

categoricalColumns = ['ALD_n', 'tranche_assiette']
stages = []
for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer().setInputCol(categoricalCol).setOutputCol(categoricalCol + 'Index')
    encoder = OneHotEncoder(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    stages += [stringIndexer, encoder]

#Recodage de la variable dépendante SEXE

label_stringIdx = StringIndexer().setInputCol("SEXE").setOutputCol("SEXE_label")
stages += [label_stringIdx]
numericCols = []
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]

#Création du pipeline (ensemble de toutes les étapes précédentes du traitement)
pipeline = pyml.Pipeline(stages = stages)
pipelineModel = pipeline.fit(t1)
t1 = pipelineModel.transform(t1)
selectedCols = ['SEXE_label', 'features'] + cols
df = t1.select(selectedCols)
df.printSchema()

#Partitionnement des données en partie pour apprentissage et partie pour test
train, test = df.randomSplit([0.7, 0.3], seed = 2018)
print("Nombre d'observations (apprentissage): " + str(train.count()))
print("Nombre d'observations (test): " + str(test.count()))

lr = LogisticRegression(featuresCol = 'features', labelCol = 'SEXE_label', maxIter=10)
lrModel = lr.fit(train)

beta = np.sort(lrModel.coefficients)
plt.plot(beta)
plt.ylabel('Beta Coefficients')
plt.show()

trainingSummary = lrModel.summary
roc = trainingSummary.roc.toPandas()
plt.plot(roc['FPR'],roc['TPR'])
plt.ylabel('False Positive Rate')
plt.xlabel('True Positive Rate')
plt.title('ROC Curve')
plt.show()
print('Training set areaUnderROC: ' + str(trainingSummary.areaUnderROC))

#Precision/recall
pr = trainingSummary.pr.toPandas()
plt.plot(pr['recall'],pr['precision'])
plt.ylabel('Precision')
plt.xlabel('Recall')
plt.show()

#predictions 
predictions = lrModel.transform(test)
predictions.show(10, truncate=False)

predictions=predictions.withColumnRenamed("SEXE_label", "label")
#evaluating the model 
evaluator = BinaryClassificationEvaluator()
print('Test Area Under ROC', evaluator.evaluate(predictions))

