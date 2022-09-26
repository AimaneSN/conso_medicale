from pyspark.sql.types import *

import  pyspark.sql.functions  as fy

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

def rename_columns(df, dict_columns : dict):
    for index in dict_columns.values():
        df = df.withColumnRenamed(df.columns[index], list(dict_columns.keys())[list(dict_columns.values()).index(index)])
    return df
