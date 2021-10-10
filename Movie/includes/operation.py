# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, current_date, explode, col, collect_set,when
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.fs.rm("dbfs:/Movie/",True)
path = "dbfs:/Movie/"
bronzePath = path + "bronze/"
silverPath = path + "silver/"
genresPath = path + "genres_lookup/"

silver_schema =["Id","Title","BackdropUrl","Budget","CreatedBy","CreatedDate","ImdbUrl","OriginalLanguage","Overview","PosterUrl","Price","ReleaseDate","Revenue","RunTime","Tagline","TmdbUrl","UpdatedBy","UpdatedDate","genres_id"]

# COMMAND ----------

def read_raw() -> DataFrame:
  jsonFile1 = "dbfs:/FileStore/tables/movie_0.json"
  jsonFile2 = "dbfs:/FileStore/tables/movie_1.json"
  jsonFile3 = "dbfs:/FileStore/tables/movie_2.json"
  jsonFile4 = "dbfs:/FileStore/tables/movie_3.json"
  jsonFile5 = "dbfs:/FileStore/tables/movie_4.json"
  jsonFile6 = "dbfs:/FileStore/tables/movie_5.json"
  jsonFile7 = "dbfs:/FileStore/tables/movie_6.json"
  jsonFile8 = "dbfs:/FileStore/tables/movie_7.json"
  movie= (spark.read           
    .option("multiline","true")  
    .json([jsonFile1,jsonFile2,jsonFile3,jsonFile4,jsonFile5,jsonFile6,jsonFile7,jsonFile8]) 
   )
  #take out the array as a single json string
  movie2 = movie.select(explode(movie.movie))
  return movie2

# COMMAND ----------

#Add metadata to bronze
def transform_raw(raw: DataFrame) -> DataFrame:
    return raw.select(
      "col",
      lit("Movie Json").alias("datasource"),
      current_timestamp().alias("ingesttime"),
      lit("new").alias("status"),
      current_date().alias("ingestdate")
    )

# COMMAND ----------

def writer(
    dataframe: DataFrame,
    path:str,
    mode:str,
    columns: List = [],
):
   return (dataframe.select(columns).write.format("delta").mode(mode).save(path))

# COMMAND ----------

def save_bronze_table():
  spark.sql("""
  DROP TABLE if exists movie_bronze
  """)

  spark.sql(f"""
  CREATE TABLE movie_bronze
  USING DELTA LOCATION "{bronzePath}"
  """)
  
  return

# COMMAND ----------

def read_bronze() -> DataFrame:
  return spark.read.table("movie_bronze").filter("status = 'new'")

# COMMAND ----------

def transform_bronze(bronzeDF: DataFrame) -> DataFrame:
  minbudget = 1000000 #set MIN budget
  
  #assign whole json to temp silver 
  silver_init = bronzeDF.select("col.*","col")
  
  #used for group array: take json out and remove duplicate and use collect_set
  genre_arr = silver_init.select("id",explode(silver_init.genres)).select(col("id").alias("movieid"),"col.id").groupBy("movieid").agg(collect_set("id").alias("genres_id"))
  
  #group genre id and drop genre json, also remove duplicates
  silver = silver_init.join(genre_arr,silver_init.Id == genre_arr.movieid,"left").drop("movieid","genres").distinct()
  
  #replace budget less than one million with 1 million 
  silver = silver.withColumn("Budget", when(silver.Budget <= 1000000,minbudget).otherwise(silver.Budget))
  
  return silver

# COMMAND ----------

#quarantine the negative runtime
def generate_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
    return (
        dataframe.filter("RunTime >0"),
        dataframe.filter("RunTime <=0"),
    )

# COMMAND ----------

def save_silver_table():
  spark.sql(
      """
  DROP TABLE IF EXISTS movie_silver
  """
  )

  spark.sql(
      f"""
  CREATE TABLE movie_silver
  USING DELTA
  LOCATION "{silverPath}"
  """
  )

# COMMAND ----------

def genres_init(bronzeDF: DataFrame) -> DataFrame:
  init = bronzeDF.select("col.*")
  genre = init.select(explode(init.genres))
  genre_clean = genre.select("col.*").distinct().filter("name !=''").sort("id")
  return genre_clean

# COMMAND ----------

def save_genres_table():
  spark.sql(
      """
  DROP TABLE IF EXISTS genres_lookup
  """
  )

  spark.sql(
      f"""
  CREATE TABLE genres_lookup
  USING DELTA
  LOCATION "{genresPath}"
  """
  )

# COMMAND ----------

def update_bronze_table_status(
    spark: SparkSession, bronzeTablePath: str, dataframe: DataFrame, status: str
) -> bool:

    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    dataframeAugmented = dataframe.withColumn("status", lit(status))

    update_match = "bronze.col = dataframe.col"
    update = {"status": "dataframe.status"}

    (
        bronzeTable.alias("bronze")
        .merge(dataframeAugmented.alias("dataframe"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True

# COMMAND ----------

def repair_quarantined_records(spark: SparkSession, bronzeTable: str) -> DataFrame:
  bronzeQ= spark.read.table(bronzeTable).filter("status = 'quarantined'") #read from bronze
  bronzeQuarantinedDF = transform_bronze(bronzeQ) #call function to clean the genre column
  repair = bronzeQuarantinedDF.withColumn("RunTime",col("RunTime")*-1) #Assign absolute value to data
  return repair