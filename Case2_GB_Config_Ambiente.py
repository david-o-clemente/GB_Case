# Databricks notebook source
pip install tweepy

# COMMAND ----------

pip install tweepy -U 

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

# MAGIC %python
# MAGIC import tweepy as tw
# MAGIC import pandas as pd
# MAGIC import os     as os
# MAGIC import numpy  as np

# COMMAND ----------

#%fs
#rm -r dbfs:/user/hive/warehouse/

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${Raw_Data_Zone};
# MAGIC CREATE DATABASE IF NOT EXISTS ${Trusted_Zone};
# MAGIC CREATE DATABASE IF NOT EXISTS ${Refined_Zone};

# COMMAND ----------

# MAGIC %python
# MAGIC Raw_Data_Zone = getArgument("Raw_Data_Zone")
# MAGIC Refined_Zone  = getArgument("Refined_Zone")
# MAGIC Trusted_Zone  = getArgument("Trusted_Zone")

# COMMAND ----------

# MAGIC %python
# MAGIC # Criando as credenciais como variaveis
# MAGIC 
# MAGIC consumer_key        = 'fgUW9sVZ77gxWAskg1VWPLNmK'
# MAGIC consumer_secret     = '0cI0eymuPYyY9XXqzqK3JXQGUddx0vPnmAOetZCy1hNthWD1us'
# MAGIC bearer_token        = 'AAAAAAAAAAAAAAAAAAAAAI1SlgEAAAAAR%2Fub6%2BhoWDOBj1Mr8ljSn8tZXSQ%3DVBJQ77xyPmIQvm9AHn9yXZ7clW9K5XkwRpsMO1NPZqgax2yssD'
# MAGIC access_token        = '138751794-VV7CLpVcxcbPhV8QWMP0sE5lu39cm4l5OaMf9DXA'
# MAGIC access_token_secret = 'vROzY5OQ7yGZCegyZYhJJC3RSUa0cyvQL4AW0b9sUIXCu'
