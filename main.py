#importation des packages

from imports import * 


sp1 = SparkSession.builder.config("spark.driver.memory", "2g").appName('conso_medicale').enableHiveSupport().getOrCreate()

#chemins des bases brutes

conso2019_path = "/home/aimane/Documents/BDDCNSS/conso2019.txt"
assiettes2019_path ="/home/aimane/Documents/BDDCNSS/assiettes_2019.txt"
ald_path = "/home/aimane/Documents/BDDCNSS/ALD_2015_2019.txt"
medic_path = "/home/aimane/Documents/BDDCNSS/médic_2019.txt"

#chemins des bases nettoyées

conso2019n_path = "/media/aimane/A376FE9926C3C949/Users/Aimane/Desktop/bases_csv/conso2019.csv"
assiettes2019n_path = "/media/aimane/A376FE9926C3C949/Users/Aimane/Desktop/bases_csv/assiettes2019.csv"
aldn_path = "/media/aimane/A376FE9926C3C949/Users/Aimane/Desktop/bases_csv/ALD2015_2019.csv"

#Importation des bases de données

base_conso = readfile.get_file(conso2019_path, ";", sp1)
base_assiette = readfile.get_file(assiettes2019_path, ";", sp1)
base_ald = readfile.get_file(ald_path, ";", sp1)
base_medic = readfile.get_file(medic_path, ";", sp1)

#Renommage des colonnes définies dans le module coldict

base_assiette = traitements.rename_columns(base_assiette, coldict.assiette_indices)
base_conso = traitements.rename_columns(base_conso, coldict.conso_indices)
base_ald = traitements.rename_columns(base_ald, coldict.ald_indices)

#Conversion vers les types appropriés

base_assiette = base_assiette.withColumn(colnames.DNAISSANCE, fy.to_timestamp(colnames.DNAISSANCE, "yyyy.MM.dd HH:mm:ss"))
base_assiette = base_assiette.withColumn(colnames.DATE_FIN_DO, fy.when(fy.length(fy.col(colnames.DATE_FIN_DO))>1,fy.to_timestamp(colnames.DATE_FIN_DO, "yyyyMMdd"))\
                                                                 .otherwise(fy.col(colnames.DATE_FIN_DO))) #date_fin_DO contient des observations égales à 0

base_assiette = traitements.convert_column(base_assiette, colnames.ASSIETTE_COTISATION, DoubleType())
base_assiette = traitements.convert_column(base_assiette, colnames.ID_BENEFICIAIRE_A, StringType())

base_conso = traitements.convert_column(base_conso, colnames.MONTANT_REMBOURSE, DoubleType())
base_conso = traitements.convert_column(base_conso, colnames.PRIX_UNITAIRE, DoubleType())
base_conso = traitements.convert_column(base_conso, colnames.DATE_DEBUT_SOIN, DateType())

base_ald = traitements.convert_column(base_ald, colnames.DATE_DEBUT_ACCORD_ALD, DateType())
base_ald = traitements.convert_column(base_ald, colnames.DATE_FIN_ACCORD_ALD, DateType())

#Création des variables AGE et TRANCHE_AGE (dans la base assiettes)

base_assiette = base_assiette.withColumn("age", fy.round(fy.months_between(fy.lit(datetime.datetime(2019,12,31)),
                                                        fy.col(colnames.DNAISSANCE))/fy.lit(12), 2))\
                           .withColumn("age", fy.when(fy.col("age")>110,110).otherwise(fy.col("age"))) 

base_assiette = base_assiette.selectExpr("*",
"CASE "
"WHEN age < 5 THEN '[0-5[' "
"WHEN age BETWEEN 5 AND 10 THEN '[5-10[' "
"WHEN age BETWEEN 10 AND 15 THEN '[10-15[' "
"WHEN age BETWEEN 15 AND 20 THEN '[15-20[' "
"WHEN age BETWEEN 20 AND 25 THEN '[20-25[' "
"WHEN age BETWEEN 25 AND 30 THEN '[25-30[' "
"WHEN age BETWEEN 30 AND 35 THEN '[30-35[' "
"WHEN age BETWEEN 35 AND 40 THEN '[35-40[' "
"WHEN age BETWEEN 40 AND 45 THEN '[40-45[' "
"WHEN age BETWEEN 45 AND 50 THEN '[45-50[' "
"WHEN age BETWEEN 50 AND 55 THEN '[50-55[' "
"WHEN age BETWEEN 55 AND 60 THEN '[55-60[' "
"WHEN age BETWEEN 60 AND 65 THEN '[60-65[' "
"WHEN age BETWEEN 65 AND 70 THEN '[65-70[' "
"WHEN age BETWEEN 70 AND 75 THEN '[70-75[' "
"WHEN age BETWEEN 75 AND 80 THEN '[75-80[' "
"WHEN age BETWEEN 80 AND 85 THEN '[80-85[' "
"WHEN age BETWEEN 85 AND 90 THEN '[85-90[' "
"WHEN age BETWEEN 90 AND 95 THEN '[90-95[' "
"WHEN age BETWEEN 95 AND 100 THEN '[95-100[' "
"WHEN age BETWEEN 100 AND 105 THEN '[100-105[' "
"WHEN age BETWEEN 105 AND 110 THEN '[105-110[' "
"WHEN age>110 THEN 110 "

"ELSE ' VALEUR AGE INVALIDE ' "

"END "
"AS tranche")

#traitement de la variable AGE

agep=Window.partitionBy(colnames.TYPE_ADHERENT, colnames.TYPE_BENEFICIAIRE, "SEXE")
base_assiette=base_assiette.withColumn("age_moyen",fy.mean("age").over(agep))


base_assiette = base_assiette.withColumn("type_benef", fy.when(fy.col(colnames.TYPE_BENEFICIAIRE)=="Adhérent", fy.col("type_adherent"))
                                                         .otherwise(fy.col(colnames.TYPE_BENEFICIAIRE)))

base_assiette=base_assiette.withColumn("age_corr", fy.when((fy.col("age") < 15) & (fy.col("type_benef")=="Actif"), 15) \
                                                        .when((fy.col("age") > 65) & (fy.col("type_benef")=="Actif"), fy.col("age_moyen")) \
                                                        .when((fy.col("age") < 16) & (fy.col("type_benef")=="Veuf"), 16) \
                                                        .when(fy.col("age") > 110, fy.col("age_moyen")) \
                                                        .otherwise(fy.col("age")))

#traitement de la variable assiette_cotisation

winpartition = Window.partitionBy(colnames.ID_ADHERENT_A).orderBy(colnames.TYPE_BENEFICIAIRE)

base_assiette=base_assiette.withColumn("assiette_adherent", fy.first(colnames.ASSIETTE_COTISATION).over(winpartition))

base_assiette=base_assiette.withColumn("assiette_corr", fy.when(fy.col(colnames.ASSIETTE_COTISATION)==0, fy.col("assiette_adherent"))
                                             .otherwise(fy.col(colnames.ASSIETTE_COTISATION)))


#annualisation des assiettes de cotisation

base_assiette = base_assiette.withColumn("assiette_cotisation_annualisee", fy.when( (fy.col(colnames.NOMBRE_MOIS).isNotNull()) & (fy.col(colnames.NOMBRE_MOIS)!=0), \
                                          fy.col(colnames.ASSIETTE_COTISATION) * 12 / fy.col(colnames.NOMBRE_MOIS))
                                          .otherwise(0))

#nombre d'actes par bénéficiaire (base conso)

benefc_partition = Window.partitionBy(colnames.ID_BENEFICIAIRE_C)
base_conso = base_conso.withColumn("nb_actes", fy.when(fy.col(colnames.ID_BENEFICIAIRE_C).isNotNull(), fy.count(colnames.ID_BENEFICIAIRE_C).over(benefc_partition)))

#Echantillons des bases initiales. Pour utiliser les bases complètes, il suffit d'effacer la partie ".limit(80000)" dans les affectations ci-après#
assiette_e = base_assiette.limit(85000)
conso_e = base_conso.limit(85000)
ald_e = base_ald.limit(85000)

#Jointures entre les bases ASSIETTES, CONSO ET ALD

##FULL JOIN
assiette_conso_e = conso_e.join(assiette_e, conso_e.id_beneficiaire_c==assiette_e.id_beneficiaire_a, "full")
assiette_conso_ald_e = assiette_conso_e.join(ald_e, assiette_conso_e.id_beneficiaire_a==ald_e.id_beneficiaire_ald, "full")

##INNER JOIN
assiette_conso_i = conso_e.join(assiette_e, conso_e.id_beneficiaire_c==assiette_e.id_beneficiaire_a, "inner")
assiette_conso_ald_i = assiette_conso_i.join(ald_e, assiette_conso_i.id_beneficiaire_a==ald_e.id_beneficiaire_ald, "full")

assiette_conso_ald_e = assiette_conso_ald_e.withColumn("ALD_n", fy.when(((fy.col(colnames.DATE_FIN_ACCORD_ALD)>=datetime.datetime(2019,1,1)) & (fy.col(colnames.DATE_DEBUT_SOIN).isNull())) 
                                    |((fy.col(colnames.DATE_DEBUT_SOIN).isNotNull()) & (fy.col(colnames.DATE_DEBUT_SOIN).between(fy.col(colnames.DATE_DEBUT_ACCORD_ALD),fy.col(colnames.DATE_FIN_ACCORD_ALD))) )
                                                                                                                                                            
, 1).otherwise(0))                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                
assiette_conso_ald_e = assiette_conso_ald_e.withColumn(colnames.ID_BENEFICIAIRE_A, fy.when(fy.col(colnames.ID_BENEFICIAIRE_A).isNull() & fy.col(colnames.ID_BENEFICIAIRE_C).isNotNull(), fy.col(colnames.ID_BENEFICIAIRE_C)).when(fy.col(colnames.ID_BENEFICIAIRE_A).isNull() & fy.col(colnames.ID_BENEFICIAIRE_ALD).isNotNull(),
fy.col(colnames.ID_BENEFICIAIRE_ALD)).otherwise(fy.col(colnames.ID_BENEFICIAIRE_A)))

assiette_conso_ald_e.coalesce(1).write.csv('base_full_join.csv', header='true')

#traitement de la variable SEXE

##Attribution du SEXE 'F' aux bénéficiaires de RANG=3,...,10

assiette_conso_ald_e=assiette_conso_ald_e.withColumn(colnames.SEXE, fy.when( assiette_conso_ald_e[colnames.RANG].between(3,10), "F").otherwise(assiette_conso_ald_e[colnames.SEXE]))

##Attribution du sexe opposé de l'adhérent au conjoint (RANG=2) (quand il est disponible) 
assiette_conso_ald_e=assiette_conso_ald_e.withColumn("SEXE_ad", fy.first(assiette_conso_ald_e["SEXE"]).over(winpartition))
assiette_conso_ald_e=assiette_conso_ald_e.withColumn("SEXE", fy.when( (assiette_conso_ald_e["RANG"]==2) & (assiette_conso_ald_e["SEXE"].isNull()) & (fy.first(assiette_conso_ald_e["SEXE"]).over(winpartition).isNotNull()), fy.first(assiette_conso_ald_e["SEXE"]).over(winpartition)).otherwise(assiette_conso_ald_e["SEXE"]))

#Fréquences de consommation

assiette_conso_ald_e.createOrReplaceTempView("base")

#Nombre d'actes médicaux par tranche d'âge, sexe, ALD/non_ALD et libellé d'acte

nb_actes=sp1.sql("SELECT sexe, ALD_n, tranche, COUNT(libelle_acte) AS NOMBRE_ACTES, COUNT (DISTINCT id_beneficiaire_a) AS EFFECTIF FROM base WHERE sexe IS NOT NULL AND tranche IS NOT NULL AND libelle_acte IS NOT NULL "
                  "GROUP BY sexe, tranche, ALD_n, libelle_acte "
                  "ORDER BY sexe, ALD_n,CASE "
                  "WHEN tranche='[0-5[' THEN 1 "
                  "WHEN tranche='[5-10[' THEN 2 "
                  "WHEN tranche='[10-15[' THEN 3 "
                  "WHEN tranche='[15-20[' THEN 4 "
                  "WHEN tranche='[20-25[' THEN 5 "
                  "WHEN tranche='[25-30[' THEN 6 "
                  "WHEN tranche='[30-35[' THEN 7 "
                  "WHEN tranche='[35-40[' THEN 8 "
                  "WHEN tranche='[40-45[' THEN 9 "
                  "WHEN tranche='[45-50[' THEN 10 "
                  "WHEN tranche='[50-55[' THEN 11 "
                  "WHEN tranche='[55-60[' THEN 12 "
                  "WHEN tranche='[60-65[' THEN 13 "
                  "WHEN tranche='[65-70[' THEN 14 "
                  "WHEN tranche='[70-75[' THEN 15 "
                  "WHEN tranche='[75-80[' THEN 16 "
                  "WHEN tranche='[80-85[' THEN 17 "
                  "WHEN tranche='[85-90[' THEN 18 "
                  "WHEN tranche='[90-95[' THEN 19 "
                  "WHEN tranche='[95-100[' THEN 20 "
                  "WHEN tranche='[100-105[' THEN 21 "
                  "WHEN tranche='[105-110[' THEN 22 "
                  "WHEN tranche=110 THEN 23 "
                  "END, CODE_ACTE")

#Effectifs des bénéficiaires  tranche d'âge, sexe, ALD/non_ALD et libellé d'acte

effectifs = sp1.sql("SELECT sexe, ALD_n, tranche, COUNT (DISTINCT id_beneficiaire_a) AS EFFECTIF FROM base WHERE SEXE IS NOT NULL AND tranche IS NOT NULL "
                  "GROUP BY SEXE, tranche, ALD_n "
                  "ORDER BY SEXE, ALD_n,CASE "
                  "WHEN tranche='[0-5[' THEN 1 "
                  "WHEN tranche='[5-10[' THEN 2 "
                  "WHEN tranche='[10-15[' THEN 3 "
                  "WHEN tranche='[15-20[' THEN 4 "
                  "WHEN tranche='[20-25[' THEN 5 "
                  "WHEN tranche='[25-30[' THEN 6 "
                  "WHEN tranche='[30-35[' THEN 7 "
                  "WHEN tranche='[35-40[' THEN 8 "
                  "WHEN tranche='[40-45[' THEN 9 "
                  "WHEN tranche='[45-50[' THEN 10 "
                  "WHEN tranche='[50-55[' THEN 11 "
                  "WHEN tranche='[55-60[' THEN 12 "
                  "WHEN tranche='[60-65[' THEN 13 "
                  "WHEN tranche='[65-70[' THEN 14 "
                  "WHEN tranche='[70-75[' THEN 15 "
                  "WHEN tranche='[75-80[' THEN 16 "
                  "WHEN tranche='[80-85[' THEN 17 "
                  "WHEN tranche='[85-90[' THEN 18 "
                  "WHEN tranche='[90-95[' THEN 19 "
                  "WHEN tranche='[95-100[' THEN 20 "
                  "WHEN tranche='[100-105[' THEN 21 "
                  "WHEN tranche='[105-110[' THEN 22 "
                  "WHEN tranche=110 THEN 23 "
                  "END") 

#######################################################################
  
#Importation de la base complète (full join et inner join), on se sert de la base inner join pour estimer les modèles

base_full_path = "/home/aimane/Documents/BDDCNSS/base_full_join_.csv"
base_inner_path = "/home/aimane/Documents/BDDCNSS/base_inner_join_.csv"

base_full = readfile.get_file(base_full_path, ",", sp1)
base_inner = readfile.get_file(base_inner_path, ",", sp1)

#Echantillon de la base 
bi = base_inner.drop("dnaissance").sample(0.008).toPandas()

#Sélection des actes médicaux dont l'effectif est supérieur à 50
l1 = bi.groupby('libelle_acte').size().sort_values(ascending=False) 
actes1 = list(l1[l1>50].index) #Actes médicaux pour lesquels l'effectif des bénéficiaires est supérieur à 50


bi_ = bi[bi["libelle_acte"].isin(actes1)]
bi_ = bi_[["id_beneficiaire_a", "sexe", "age", "libelle_acte", "ALD_n", "code_ald_alc", "nb_actes"]].drop_duplicates().drop(columns = ["id_beneficiaire_a"])


bi_["code_ald_alc"][bi_["code_ald_alc"].isnull()] = "Aucun"

#Création des dummy variable (prennent les valeurs 1 ou 0 pour chaque catégorie)
bi__ = pd.concat([bi_, pd.get_dummies(bi_["libelle_acte"], drop_first = True)], axis = 1).drop(columns = ["libelle_acte"])
bi__ = pd.concat([bi_, pd.get_dummies(bi_["libelle_acte"], drop_first = True), pd.get_dummies(bi_["code_ald_alc"], drop_first = True)], axis = 1).drop(columns = ["libelle_acte", "code_ald_alc"])

ya = bi__[["age"]]

X = bi__.drop(columns = ["age", "sexe"]).reset_index().drop(columns = ["index"])

X = sm.add_constant(X)
X_train, X_test, Y_train, Y_test = train_test_split(X.reset_index().drop(columns = ["index"]), ya.reset_index().drop(columns = ["index"]), test_size = 0.30)

glm_a = sm.OLS(Y_train, X_train.drop(columns = ["ALD_n"]) )
res = glm_a.fit()
sum1 = res.summary()

# Exportation des résultats de la régression

s_file = open("summary1.csv", "w")
s_file.write(sum1.as_csv())
s_file.close()



# #RF(pyspark) : SEXE ~ LIBELLE_ACTE
# libelles = base_inner.groupBy("libelle_acte").agg(fy.count("libelle_acte").alias("N")).filter(fy.col("N")>100).select("libelle_acte")
# l_lib = list(libelles.toPandas()['libelle_acte'])

# f_bi = base_inner.filter(fy.col("libelle_acte").isin(l_lib))

# train, test = f_bi.limit(600000).randomSplit([0.7, 0.3], seed = 2022)

# indexed_lib = StringIndexer().setInputCol("libelle_acte").setOutputCol("libelle_id")

# indexed_sexe = StringIndexer().setInputCol("sexe").setOutputCol("sexe_id")

# f_bi_ = indexed_lib.fit(train).transform(train)
# f_bi_ = indexed_sexe.fit(f_bi_).transform(f_bi_)

# ohe=pyml.feature.OneHotEncoder()
# libelle_ohe=ohe.setInputCols(['libelle_id']).setOutputCols(['libelle_e'])

# encoder_m=libelle_ohe.fit(f_bi_)
# f_bi_=encoder_m.transform(f_bi_)


# f_bi_ = f_bi_.select("id_beneficiaire_a","sexe", "libelle_acte", "sexe_id", "libelle_id", "libelle_e")

# rf = RandomForestClassifier(featuresCol = 'libelle_e', labelCol = 'sexe_id')

# rfModel = rf.fit(f_bi_)
# predictions = rfModel.transform(test)

# evaluator = MulticlassClassificationEvaluator(labelCol="sexe_id", predictionCol="prediction")
# accuracy = evaluator.evaluate(predictions)
# print("Accuracy = %s" % (accuracy))
# print("Test Error = %s" % (1.0 - accuracy))
