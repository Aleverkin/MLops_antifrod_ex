import findspark
findspark.init()
findspark.find()

import pandas as pd
import pickle
import datetime

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import pyspark.sql.functions as f
from pyspark.sql.types import *

spark = (
    SparkSession
        .builder
        .appName("OTUS")
        .config('spark.executor.cores', '2')
        .config('spark.executor.instances', '4')
        .config("spark.executor.memory", "3g")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
)

print('Starting context ...')
sql = SQLContext(spark)
print('Context started.')

# Читаем файлы
with open("name_files.pickle", "rb") as output_file:
    name_files = pickle.load(output_file)
    
# Создаем функцию чтения датафреймов
struct = StructType([
    StructField("tranaction_id", IntegerType(), nullable = True),
    StructField("tx_datetime", StringType(), nullable = True), 
    StructField("customer_id", IntegerType(), nullable = True), 
    StructField("terminal_id", IntegerType(), nullable = True),
    StructField("tx_amount", DoubleType(), nullable = True), 
    StructField("tx_time_seconds", IntegerType(), nullable = True), 
    StructField("tx_time_days", IntegerType(), nullable = True),
    StructField("tx_fraud", IntegerType(), nullable = True), 
    StructField("tx_fraud_scenario", IntegerType(), nullable = True)])

def read_df(name):
    df = spark.read\
          .option("header", "true")\
          .option("inferSchema", "false")\
          .option("delimiter", ",")\
          .schema(struct)\
          .csv("s3a://hw2-mlops-course/" + name)
    
    return df

# Проверим на:

# 1. Пропущенные числа
#     1. Для численных ['tx_amount'] - среднее
#     2. Для fraud/tranaction_id/категориальных - удаляем вприцнипе, чтобы не загрязнял датасет (данных достаточно)
#     3. Для datettime - заполняем датой из названия датасета
# 2. Выбросы - будем заменять 99% перцентилем

for name_file in name_files:
    print(f'Start clean file: {name_file}, time: {datetime.datetime.today()}') 
    
    df = read_df(name_file)
    
    # 1. На всякий проверяем на NaN
    cols_nan_drop = ['tx_fraud', 'tx_fraud_scenario', 'tranaction_id', 'customer_id', 'terminal_id']
    df = df.dropna(subset=cols_nan_drop)
    
    # 2. Убираем Null
    df = df.filter(f.col('customer_id').isNotNull())
    
    # 3. Id только положительные
    df = df.filter(f.col('customer_id') >= 0)
    
    # 4. Если пропущены секунды или дни - заменяем на расчет из комплементарного столбца
    df = df.filter((f.col('tx_time_seconds').isNotNull()) | (f.col('tx_time_days').isNotNull()))
   
    df = df.withColumn('tx_time_days_calc', (f.col('tx_time_seconds') / 3600 / 24).cast(IntegerType()))\
            .withColumn('tx_time_days', f.coalesce('tx_time_days', 'tx_time_days_calc'))
    
    df = df.withColumn('tx_time_seconds_calc', (f.col('tx_time_days') * 3600 * 24).cast(IntegerType()))\
            .withColumn('tx_time_seconds', f.coalesce('tx_time_seconds', 'tx_time_seconds_calc'))
    
    df = df.drop(*['tx_time_days_calc', 'tx_time_seconds_calc'])
    
    # 5. Заполняем пропуски в дате на название файла (на всякий)s
    year = int(name_file[:4])
    month = int(name_file[5:7])
    day = int(name_file[8:10])
    
    datetime_miss = f"{year}-{month}-{day} 00:00:00"
    
    df = df.fillna(datetime_miss, subset=['tx_datetime'])
    
    #6. Если дата не совпадает с названием файла - убираем такие строчки
    split_col = f.split(df['tx_datetime'], ' ')
    df = df.withColumn('tx_date', split_col.getItem(0))
    
    df = df.filter(f.col('tx_date') == name_file[:-4])
    df = df.drop('tx_date')
    
    #7. Заменяем выбросы на 99% перцентиль
    for col in ['tx_amount', 'tx_time_seconds', 'tx_time_days']:
        perc_99 = df.approxQuantile(col, [0.99], 0.1)[0]

        df = df.withColumn(col, f.when(f.col(col) > perc_99, perc_99).otherwise(f.col(col)))
    
    #9. Сохраняем
    df.repartition(1).write.parquet(f"s3a://mlops-parsed-data/data_parquet/{name_file[:-4]}.parquet", mode="overwrite")
    
    print(f'Finish clean file: {name_file}', '\n', '*' * 70, '\n') 
    


