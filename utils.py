import coldict
import colnames 
import  pyspark.sql.functions  as F
from pyspark.sql.types import *

def rename_columns(df, dict_columns : dict):
    #Renommage des colonnes avec le noms et les indices figurant dans dict_columns

    for index in dict_columns.values():

        df = df.withColumnRenamed(df.columns[index], list(dict_columns.keys())[list(dict_columns.values()).index(index)])
        
    return df

def convert_columns(df, dict_types : dict):
    #Conversion des colonnes vers les types définis dans dict_types

    cols = list(dict_types.keys())
    for col in cols:

        if dict_types[col] == coldict.SPARK_STRING:
            df = df.withColumn(col, df[col].cast(StringType()))

        elif dict_types[col] == coldict.SPARK_INT:
            df = df.withColumn(col, df[col].cast(IntegerType()))

        elif dict_types[col] == coldict.SPARK_DOUBLE:
            df = df.withColumn(col, df[col].cast(StringType()))
            df = df.withColumn(col, F.regexp_replace(F.trim(col), ",", ".")) 
            df = df.withColumn(col, df[col].cast(DoubleType()))

        elif dict_types[col] == coldict.SPARK_DATE:
            print(coldict.dates_formats_dict)
            df = df.withColumn(col, df[col].cast(StringType()))
            df = df.withColumn(col, F.to_date(F.to_timestamp(df[col], coldict.dates_formats_dict[col])))
    
    return df

def convert_column(df, column : str, coltype : str):

    if coltype == coldict.SPARK_STRING:
        df = df.withColumn(column, df[column].cast(StringType()))

    elif coltype == coldict.SPARK_INT:
        df = df.withColumn(column, df[column].cast(IntegerType()))

    elif coltype == coldict.SPARK_DOUBLE:
        df = df.withColumn(column, df[column].cast(StringType()))
        df = df.withColumn(column, F.regexp_replace(column, ",", ".")) 
        df = df.withColumn(column, df[column].cast(DoubleType()))

    elif coltype == coldict.SPARK_DATE:
        df = df.withColumn(column, F.trim(df[column]).cast(StringType()))
        df = df.withColumn(column, F.to_date(F.to_timestamp(df[column], coldict.dates_formats_dict[column])))
        print(coldict.dates_formats_dict[column])
    
    return df

def describe_column(df, colname : str):
    #Statistiques descriptives pour les variables numériques

    str_count = colname + "_count"
    str_avg = colname + "_avg"
    str_min = colname + "_min"
    str_max = colname + "_max"
    str_sum = colname + "_sum"
    
    resultats_describe = df.groupBy(colnames.TYPE_ADHERENT, colnames.TYPE_BENEFICIAIRE, colnames.SEXE).agg(F.count(colname).alias(str_count),
                                                                                             F.avg(colname).alias(str_avg), 
                                                                                             F.min(colname).alias(str_min),
                                                                                             F.max(colname).alias(str_max),
                                                                                             F.sum(colname).alis(str_sum))

    return resultats_describe

