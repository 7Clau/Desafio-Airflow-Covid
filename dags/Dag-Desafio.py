from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_timestamp, month, year, to_date, sum, floor, avg, round
from pyspark.sql.window import Window
import os

default_args = {
    'owner': 'Claudio',
    'email': ['claudio@gmail.com'],
    'start_date': days_ago(1),
    'email_on_failure': False
}

### ----- Inicio das Funções ----- ####

def create_spark_session(app_name):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName(app_name) \
        .config('spark.executor.memory', '6g') \
        .config('spark.driver.memory', '6g') \
        .config("spark.driver.maxResultSize", "1048MB") \
        .config("spark.port.maxRetries", "100") \
        .getOrCreate()
    return spark

def realiza_pivot(df, tipo):
    # Separa as colunas datas para realizar pivot
    colunas = df.columns[4:]

    # Coleta a quantidade de colunas que farão pivot
    n = len(colunas)
    
    # Expressao para realizar o pivot
    expr_str = "stack({}, {}) as (data, {})".format(n, ", ".join(["'{}', `{}`".format(col, col) for col in colunas]), tipo)
    
    # Realiza o pivot - transforma as colunas datas em registros
    df = df.selectExpr("estado", "pais", "latitude", "longitude", expr_str)
    return df

def renomea_colunas(df):
    df = df.withColumnRenamed("Province/State", "estado") \
        .withColumnRenamed("Country/Region", "pais") \
        .withColumnRenamed("Lat", "latitude") \
        .withColumnRenamed("Long", "longitude")
    return df

def trunca_colunas(df):
    # Floor retorna o valor inteiro da multiplicação longitude * 100 e após divido por 100 para ficar com 2 casas decimais
    df = df.withColumn("longitude", floor(df["longitude"] * 100) / 100).withColumn("latitude", floor(df["latitude"] * 100) / 100)
    return df

def checa_arquivos(path1, path2, path3):
    file_paths = [path1, path2, path3]
    for file_path in file_paths:
        if not os.path.exists(file_path):
            raise FileNotFoundError("Arquivo não encontrado: {}".format(file_path))

def processa_raw(diretorio, formato, tipo, **kwargs):
    spark = create_spark_session(tipo)
    df = spark.read.options(header='true', inferSchema=True).format(formato).load(diretorio)

    # Funções de preparação e conformidade
    df = renomea_colunas(df)
    df = trunca_colunas(df)
    df = realiza_pivot(df, tipo)

    # Print dos dados(filtrado pelo pais armenia e ordenado pela data)
    df.filter((df["pais"] == "Armenia")).orderBy(col("data").desc()).show(5)

    # Grava os dados intermediaros na camada raw
    df.write.format("parquet").option("header", "true").mode("overwrite").save("/home/airflow/datalake/raw/" + tipo)

    spark.stop()

def processa_trusted():
    spark = create_spark_session("trusted")

    # Leitura dos dados para realizar o join entre os três arquivos
    mortos = spark.read.options(header='true',inferSchema=True).parquet("/home/airflow/datalake/raw/quantidade_mortes")
    recuperados = spark.read.options(header='true',inferSchema=True).parquet("/home/airflow/datalake/raw/quantidade_recuperados")
    confirmados = spark.read.options(header='true',inferSchema=True).parquet("/home/airflow/datalake/raw/quantidade_confirmados")

    # Inner join entre as tabelas deaths e confirmed
    join = confirmados.join( mortos,((confirmados.latitude == mortos.latitude) & (confirmados.longitude == mortos.longitude)  & (confirmados.data == mortos.data) & (confirmados.pais == mortos.pais)), "inner" ) \
        .select(mortos["*"], confirmados["quantidade_confirmados"])

    # Right join entre as tabelas join e recovered
    join = recuperados.join(join,((recuperados.latitude == join.latitude) & (recuperados.longitude == join.longitude)  & (recuperados.data == join.data) & (recuperados.pais == join.pais)), "right" ) \
        .select(join["*"], recuperados["quantidade_recuperados"])
    
    # Altera os tipos de dados
    trusted = join.withColumn("data", to_timestamp(col("data"), "M/d/yy")) \
        .withColumn("quantidade_mortes", col("quantidade_mortes").cast("long")) \
        .withColumn("quantidade_confirmados", col("quantidade_confirmados").cast("long")) \
        .withColumn("quantidade_recuperados", col("quantidade_recuperados").cast("long")) \
        .withColumn("mes", month("data")) \
        .withColumn("ano", year("data"))

    # Print dos dados da camada trustud (filtrado pelo pais armenia e ordenado pela data)
    trusted.filter((trusted["pais"] == "Armenia")).orderBy(col("data").desc()).show(10)

    # Print dos metadados da camada trusted
    print("Tipagem de dados: "+ str(trusted))

    # Grava os dados em um unico arquivo particionado por ano e mes no formato parquet
    trusted.repartition(1).write.format("parquet").option("header", "true").mode("overwrite").partitionBy("ano","mes").save("/home/airflow/datalake/trusted")
    spark.stop()

def processa_refined():
    spark = create_spark_session("refined")
    df = spark.read.options(header='true',inferSchema=True).parquet("/home/airflow/datalake/trusted")

    # Seleção dos dados para a camada refined
    df = df.select("pais","data", "quantidade_confirmados", "quantidade_mortes", "quantidade_recuperados", "ano")

    # Agregação dos dados por pais e data
    agg = df.groupBy("pais","data","ano").agg(sum("quantidade_confirmados"), sum("quantidade_mortes"), sum("quantidade_recuperados")).orderBy("data")

    # Cria janela particionada por pais e ordenada por data com intervalo de 7 valores
    window = Window.partitionBy("pais").orderBy("data").rowsBetween(-6, 0)

    # Cria coluna media movel e faz o arredondamente para duas casas decimais
    refined_media_movel = agg.withColumn("media_movel_mortes", round(avg(col("sum(quantidade_mortes)")).over(window), 2)) \
        .withColumn("media_movel_confirmados", round(avg(col("sum(quantidade_confirmados)")).over(window), 2)) \
        .withColumn("media_movel_recuperados", round(avg(col("sum(quantidade_recuperados)")).over(window), 2))

    # Selecão de colunas e cast das colunas para long
    refined = refined_media_movel.select("pais","data","media_movel_confirmados", "media_movel_mortes","media_movel_recuperados", "ano") \
        .withColumn("media_movel_mortes", col("media_movel_mortes").cast("long")) \
        .withColumn("media_movel_confirmados", col("media_movel_confirmados").cast("long")) \
        .withColumn("media_movel_recuperados", col("media_movel_recuperados").cast("long"))

    # Print dos dados da camada refined (filtrado pelo pais armenia e ordenado pela data)
    refined.filter((refined["pais"] == "Armenia")).orderBy(col("data").desc()).show(10)

    # Print dos metadados da camada refined
    print("Tipagem de dados: "+ str(refined))

    # Grava os dados em um unico arquivo particionando por ano no formato parquet na camada refinada
    refined.repartition(1).write.format("parquet").option("header", "true").mode("overwrite").partitionBy("ano").save("/home/airflow/datalake/refined")
    spark.stop()


### ----- Inicio das dags ----- ####

with DAG(
    dag_id='Dag-Desafio',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    schedule_interval= timedelta(days=1),
    tags=['desafio']
) as dag:

    # Função que verifica a disponibilidade dos arquivos da covid na camada raw
    checa_arquivos = PythonOperator(
        task_id = 'check_files',
        python_callable = checa_arquivos,
        op_kwargs = {   'path1': '/home/airflow/datalake/raw/covid19/time_series_covid19_deaths_global.csv',
                        'path2': '/home/airflow/datalake/raw/covid19/time_series_covid19_recovered_global.csv',
                        'path3': '/home/airflow/datalake/raw/covid19/time_series_covid19_confirmed_global.csv'}
    )

    # Realiza processamento da camada raw time_series_covid19_deaths_global.csv
    processa_mortos = PythonOperator(
        task_id='raw_deaths',
        python_callable=processa_raw,
        op_kwargs={'diretorio': '/home/airflow/datalake/raw/covid19/time_series_covid19_deaths_global.csv', 'formato': 'csv', 'tipo': 'quantidade_mortes'}
    )

    # Realiza processamento da camada raw time_series_covid19_recovered_global.csv
    processa_recuperados = PythonOperator(
        task_id='raw_recovered',
        python_callable=processa_raw,
        op_kwargs={'diretorio': '/home/airflow/datalake/raw/covid19/time_series_covid19_recovered_global.csv', 'formato': 'csv', 'tipo': 'quantidade_recuperados'}
    )

    # Realiza processamento da camada raw time_series_covid19_confirmed_global.csvv
    processa_confirmados = PythonOperator(
        task_id='raw_confirmed',
        python_callable=processa_raw,
        op_kwargs={'diretorio': '/home/airflow/datalake/raw/covid19/time_series_covid19_confirmed_global.csv', 'formato': 'csv', 'tipo': 'quantidade_confirmados'}
    )

    # Realiza o processamento dos dados tratados da camada raw para a camada Trusted
    trusted = PythonOperator(
        task_id='trusted',
        python_callable=processa_trusted
    )

    # Realiza o processamento dos dados tratados da camada Trusted para a camada Refined
    refined = PythonOperator(
        task_id='refined',
        python_callable=processa_refined
    )

    checa_arquivos >> (processa_mortos,processa_confirmados, processa_recuperados) >> trusted >> refined
