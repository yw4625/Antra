# Databricks notebook source
# MAGIC %run ./includes/operation

# COMMAND ----------

#Raw to Bronze 
raw = read_raw() #read from json
bronze = transform_raw(raw) #add metadata
writer(bronze,bronzePath, "overwrite", ["datasource","ingesttime", "col", "status", "ingestdate"]) #dbfs table
save_bronze_table() #sql table

#Bronze to silver table and quarantine data
bronzeDF = read_bronze()
transformedBronzeDF = transform_bronze(bronzeDF)
(silver_clean, silver_quarantine) = generate_clean_and_quarantine_dataframes(transformedBronzeDF)

#create genres table
genres_table = genres_init (bronzeDF) #transofrm genres
writer(genres_table,genresPath, "overwrite", ["id","name"]) #dbfs table
save_genres_table() #sql table

#write to silver
writer(silver_clean,silverPath, "overwrite", silver_schema) #dbfs table
save_silver_table() #sql table

#update bronze table staus
update_bronze_table_status(spark, bronzePath, silver_clean, "loaded")
update_bronze_table_status(spark, bronzePath, silver_quarantine, "quarantined")

#repair quarantined data, then write to silver, finally update the bronze table status
silverCleanedDF = repair_quarantined_records(spark,"movie_bronze")
writer(silverCleanedDF,silverPath, "append", silver_schema) #dbfs table
update_bronze_table_status(spark, bronzePath, silverCleanedDF, "loaded")

# COMMAND ----------

