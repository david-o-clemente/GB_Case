-- Databricks notebook source
-- MAGIC %md
-- MAGIC # PARTE 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuracao do ambiente

-- COMMAND ----------

-- MAGIC %run
-- MAGIC /Users/david_4787@hotmail.com/GB/Case2_GB_Config_Ambiente

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime
-- MAGIC 
-- MAGIC #Guardar tempo de inicio de execucao
-- MAGIC inicio_parte_1 = datetime.now()
-- MAGIC inicio = datetime.now()

-- COMMAND ----------

CREATE WIDGET TEXT Raw_Data_Zone DEFAULT 'Bronze';
CREATE WIDGET TEXT Trusted_Zone  DEFAULT 'Silver';
CREATE WIDGET TEXT Refined_Zone  DEFAULT 'Gold';

--REMOVE WIDGET Raw_Data_Zone

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Raw Data Zone (Bronze layer)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC inicio_Raw_Data_Zone = datetime.now()

-- COMMAND ----------

-- DBTITLE 1,Extração de todos os arquivos da origem EMAIL
-- MAGIC %python
-- MAGIC 
-- MAGIC # Definicao de variaveis
-- MAGIC path                 = '/dbfs/mnt/gb/email/'
-- MAGIC isHeaderOn           = "true"
-- MAGIC isInferSchemaOn      = "false"
-- MAGIC sistema_origem_case  = "EMAIL"
-- MAGIC tipo_arq_origem_case = "XLSX"
-- MAGIC 
-- MAGIC # Definacao de listas
-- MAGIC lista = []
-- MAGIC cols_category = ['MARCA', 'LINHA', 'QTD_VENDA','SISTEMA_ORIG','TIPO_ARQV_ORIG']
-- MAGIC 
-- MAGIC 
-- MAGIC df_extract_bases = pd.DataFrame()
-- MAGIC 
-- MAGIC for f in os.listdir(path):
-- MAGIC         frames  = pd.read_excel(path + f, header= 0,dtype={'ID_MARCA': np.object,'MARCA': np.object,'ID_LINHA': np.object,'LINHA': np.object,'DATA_VENDA': np.object,'QTD_VENDA': np.object})
-- MAGIC         df_extract_bases = df_extract_bases.append(frames, ignore_index=True)
-- MAGIC         
-- MAGIC # Criacao de colunas importantes na camada de extracao (Raw_Data_Zone/Bronze)
-- MAGIC df_extract_bases["SISTEMA_ORIG"]   = sistema_origem_case
-- MAGIC df_extract_bases["TIPO_ARQV_ORIG"] = tipo_arq_origem_case
-- MAGIC df_extract_bases[cols_category]    = df_extract_bases[cols_category].astype('category')
-- MAGIC df_extract_bases["DT_HR_PROC"]     = pd.Timestamp.today()     
-- MAGIC 
-- MAGIC df_extract_bases["CHAVE_EXTRACT_BASES"] = df_extract_bases["ID_MARCA"].astype(str) +"-"+ df_extract_bases["MARCA"].astype(str) +"-"+ df_extract_bases["ID_LINHA"].astype(str) +"-"+df_extract_bases["LINHA"].astype(str) +"-"+ df_extract_bases["DATA_VENDA"].astype(str) +"-"+ df_extract_bases["QTD_VENDA"].astype(str) +"-"+ df_extract_bases["SISTEMA_ORIG"].astype(str)
-- MAGIC 
-- MAGIC df_extract_bases.info()
-- MAGIC 
-- MAGIC df_extract_bases_final = spark.createDataFrame(df_extract_bases)
-- MAGIC 
-- MAGIC # Criacao tabela no Catalog hive_metastore Raw_Data_Zone.base
-- MAGIC ( df_extract_bases_final.write.mode("overwrite")
-- MAGIC                         .option("overwriteSchema", "true")
-- MAGIC                         .saveAsTable(Raw_Data_Zone+".Base") )
-- MAGIC 
-- MAGIC # Validacao do total de registros inseridos na tabela Bronze.Base
-- MAGIC display(spark.sql(f"""SELECT COUNT(*) FROM {Raw_Data_Zone}.Base"""))
-- MAGIC 
-- MAGIC display(df_extract_bases_final)
-- MAGIC 
-- MAGIC # Criando comentario na tabela
-- MAGIC df_extract_bases_final = spark.sql(f""" COMMENT ON TABLE {Raw_Data_Zone}.Base IS 'Criacao da tabela BASES, onde ococrre a unificacao de todas as entradas de arquivos BASES_ANO*, sem tratamento e inseridos no Raw_Data_Zone (Bronze)'
-- MAGIC                                    """)

-- COMMAND ----------

-- DBTITLE 1,Realização de melhoria de processamento
-- MAGIC %md
-- MAGIC 3000 entries memory usage: 234.5+ KB antes da conversao dos campos  para categoria</br>
-- MAGIC 3000 entries memory usage: 214.2+ KB MARCA </br>
-- MAGIC 3000 entries memory usage: 193.9+ KB MARCA | LINHA </br>
-- MAGIC 3000 entries memory usage: 174.1+ KB MARCA | LINHA | QTD_VENDA</br>
-- MAGIC 3000 entries memory usage: 153.7+ KB MARCA | LINHA | QTD_VENDA | SISTEMA_ORIG</br>
-- MAGIC 3000 entries memory usage: 133.3+ KB MARCA | LINHA | QTD_VENDA | SISTEMA_ORIG | TIPO_ARQV_ORIG </br>
-- MAGIC 
-- MAGIC **_Aproximadamente 57% de reducao de consumo de memoria no processamento da ingestao das tabelas bases._**

-- COMMAND ----------

-- DBTITLE 1,Tempo de execucao total Raw Data Zone
-- MAGIC %python
-- MAGIC 
-- MAGIC final_Raw_Data_Zone   = datetime.now()
-- MAGIC 
-- MAGIC duracao_Raw_Data_Zone = final_Raw_Data_Zone - inicio_Raw_Data_Zone
-- MAGIC 
-- MAGIC print(duracao_Raw_Data_Zone)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Trusted Zone (Camada Silver)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC inicio_Trusted_Zone = datetime.now()

-- COMMAND ----------

-- DBTITLE 1,DDL - ${Trusted_Zone}.BASE
CREATE TABLE IF NOT EXISTS ${Trusted_Zone}.BASE ( ID_MARCA INT,
                                                  MARCA STRING,
                                                  ID_LINHA INT,
                                                  LINHA STRING,
                                                  DATA_VENDA DATE,
                                                  QTD_VENDA INT,
                                                  SISTEMA_ORIG STRING,	
                                                  TIPO_ARQV_ORIG STRING,
                                                  DT_HR_PROC TIMESTAMP,
                                                  CHAVE_EXTRACT_BASES STRING)
USING DELTA               
COMMENT 'Criacao da tabela BASES na camada Trusted_Zone (Silver) , ocorrendo tratamento de datatypes dos campos';      

DESC EXTENDED ${Trusted_Zone}.BASE;

-- COMMAND ----------

MERGE INTO ${Trusted_Zone}.BASE BASE

USING (SELECT 
              ID_MARCA
             ,MARCA
             ,ID_LINHA
             ,LINHA
             ,DATA_VENDA
             ,QTD_VENDA
             ,SISTEMA_ORIG
             ,TIPO_ARQV_ORIG
             ,DT_HR_PROC
             ,CHAVE_EXTRACT_BASES

       FROM (SELECT DISTINCT
                    INT(ID_MARCA)                    AS ID_MARCA
                   ,MARCA                            AS MARCA
                   ,INT(ID_LINHA)                    AS ID_LINHA
                   ,LINHA                            AS LINHA
                   ,TO_DATE(DATA_VENDA,'yyyy-MM-dd') AS DATA_VENDA
                   ,INT(QTD_VENDA)                   AS QTD_VENDA
                   ,SISTEMA_ORIG                     AS SISTEMA_ORIG
                   ,TIPO_ARQV_ORIG                   AS TIPO_ARQV_ORIG
                   ,TIMESTAMP(DT_HR_PROC)            AS DT_HR_PROC
                   ,CHAVE_EXTRACT_BASES              AS CHAVE_EXTRACT_BASES
                   ,ROW_NUMBER() OVER (PARTITION BY CHAVE_EXTRACT_BASES ORDER BY CHAVE_EXTRACT_BASES ) AS CORTE_DUP
                                           
             FROM ${Raw_Data_Zone}.BASE )
             
             WHERE CORTE_DUP = 1 ) RAW_BASE
              
 ON UPPER(TRIM(BASE.CHAVE_EXTRACT_BASES)) = UPPER(TRIM(RAW_BASE.CHAVE_EXTRACT_BASES))

WHEN NOT MATCHED THEN INSERT * ;

--SELECT COUNT(*) FROM ${Trusted_Zone}.BASE;

SELECT * FROM ${Trusted_Zone}.BASE;

-- COMMAND ----------

-- SELECT * FROM (
-- SELECT
--         ID_MARCA
--        ,MARCA
--        ,ID_LINHA
--        ,LINHA
--        ,DATA_VENDA
--        ,QTD_VENDA
--        ,SISTEMA_ORIG
--        ,TIPO_ARQV_ORIG
--        ,DT_HR_PROC
--        ,CHAVE_EXTRACT_BASES
--        ,ROW_NUMBER() OVER (PARTITION BY CHAVE_EXTRACT_BASES ORDER BY CHAVE_EXTRACT_BASES ) AS CORTE_DUP
                                           
-- FROM ${Raw_Data_Zone}.BASE )

-- WHERE (CORTE_DUP) > 1

-- COMMAND ----------

-- SELECT
-- *
-- FROM ${Raw_Data_Zone}.BASE
-- WHERE CHAVE_EXTRACT_BASES  ='5-BELEZA NA WEB-3-MAQUIAGEM-2019-06-14-11-EMAIL'

-- COMMAND ----------

-- SELECT 
--  (SELECT COUNT(*) FROM ${Raw_Data_Zone}.BASE) AS COUNT_BRONZE_BASES
-- ,(SELECT COUNT(*) FROM ${Trusted_Zone}.BASE)  AS COUNT_SILVER_BASES

-- FROM ${Raw_Data_Zone}.BASE
--     ,${Trusted_Zone}.BASE

-- GROUP BY COUNT_BRONZE_BASES,COUNT_SILVER_BASES

-- COMMAND ----------

-- DBTITLE 1,Tempo de execucao total Trusted_Zone
-- MAGIC %python
-- MAGIC 
-- MAGIC final_Trusted_Zone = datetime.now()
-- MAGIC 
-- MAGIC duracao_Trusted_Zone = final_Trusted_Zone - inicio_Trusted_Zone
-- MAGIC 
-- MAGIC print(duracao_Trusted_Zone)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Refined Zone (Camada Gold)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC inicio_Refined_Zone = datetime.now()

-- COMMAND ----------

-- DBTITLE 1,a. Tabela1: Consolidado de vendas por ano e mês;
DROP TABLE IF EXISTS ${Refined_Zone}.TABELA_1;

CREATE TABLE ${Refined_Zone}.TABELA_1 
USING DELTA AS 
SELECT
       DATE_FORMAT(LAST_DAY(DATA_VENDA),"yyyy-MM") AS ANO_MES
      ,SUM(QTD_VENDA)                              AS SUM_OF_VENDAS
      
FROM ${Trusted_Zone}.BASE   

GROUP BY LAST_DAY(DATA_VENDA)

ORDER BY LAST_DAY(DATA_VENDA);

SELECT * FROM ${Refined_Zone}.TABELA_1;

-- COMMAND ----------

-- DBTITLE 1,b. Consolidado de vendas por marca e linha;
DROP TABLE IF EXISTS ${Refined_Zone}.TABELA_2;

CREATE TABLE ${Refined_Zone}.TABELA_2 
USING DELTA AS 
SELECT
       MARCA
      ,LINHA 
      ,SUM(QTD_VENDA) AS SUM_OF_VENDAS
      
FROM ${Trusted_Zone}.BASE   

GROUP BY MARCA,LINHA

ORDER BY MARCA,LINHA;

SELECT * FROM ${Refined_Zone}.TABELA_2;

-- COMMAND ----------

-- DBTITLE 1,c. Tabela3: Consolidado de vendas por marca, ano e mês;
DROP TABLE IF EXISTS ${Refined_Zone}.tabela_3;

CREATE TABLE ${Refined_Zone}.TABELA_3 
USING DELTA AS 
SELECT
       MARCA
      ,DATE_FORMAT(LAST_DAY(DATA_VENDA),"yyyy-MM") AS ANO_MES
      ,SUM(QTD_VENDA)                              AS SUM_OF_VENDAS
      
FROM ${Trusted_Zone}.BASE   

GROUP BY MARCA,LAST_DAY(DATA_VENDA)

ORDER BY MARCA,LAST_DAY(DATA_VENDA);

SELECT * FROM ${Refined_Zone}.TABELA_3;

-- COMMAND ----------

-- DBTITLE 1,d. Tabela4: Consolidado de vendas por linha, ano e mês; 
DROP TABLE IF EXISTS ${Refined_Zone}.TABELA_4;

CREATE TABLE ${Refined_Zone}.TABELA_4 
USING DELTA AS 
SELECT
       LINHA
      ,DATE_FORMAT(LAST_DAY(DATA_VENDA),"yyyy-MM") AS ANO_MES
      ,SUM(QTD_VENDA)                              AS SUM_OF_VENDAS
      
FROM ${Trusted_Zone}.BASE   

GROUP BY LINHA,LAST_DAY(DATA_VENDA)

ORDER BY LINHA,LAST_DAY(DATA_VENDA);

SELECT * FROM ${Refined_Zone}.TABELA_4;

-- COMMAND ----------

-- DBTITLE 1,Query com a reposta do Case2
SELECT
       LINHA
      ,SUM_OF_VENDAS
      
FROM ${Refined_Zone}.TABELA_4

WHERE ANO_MES = TO_DATE('2019-12','yyyy-MM')

ORDER BY SUM_OF_VENDAS DESC

LIMIT 1;

-- COMMAND ----------

-- DBTITLE 1,Tempo de execucao total Refined_Zone
-- MAGIC %python
-- MAGIC 
-- MAGIC final_Refined_Zone = datetime.now()
-- MAGIC 
-- MAGIC duracao_Refined_Zone = final_Refined_Zone  - inicio_Refined_Zone
-- MAGIC 
-- MAGIC print(duracao_Refined_Zone)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC FIM DO PROCESSO PARTE 1

-- COMMAND ----------

-- DBTITLE 1,Tempo total do processo - PARTE 1
-- MAGIC %python
-- MAGIC final_parte_1 = datetime.now()
-- MAGIC duracao_parte_1 = final_parte_1 - inicio_parte_1
-- MAGIC print(duracao_parte_1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # PARTE 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC inicio_parte_2 = datetime.now()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta API Twitter

-- COMMAND ----------

-- DBTITLE 1,Obs: A API só mostra os tweets dos últimos 7 dias.
-- MAGIC %python
-- MAGIC 
-- MAGIC query_list = '(Boticário maquiagem) lang:pt'
-- MAGIC #query_list = '(Boticário) lang:pt'
-- MAGIC 
-- MAGIC cliente = tw.Client(bearer_token, consumer_key, consumer_secret, access_token)
-- MAGIC 
-- MAGIC tweets = cliente.search_recent_tweets(query=query_list, user_fields=['username','name'], tweet_fields = 'text', expansions = 'author_id', max_results = 50)
-- MAGIC 
-- MAGIC # Get users list from the includes object
-- MAGIC users = {u["id"]: u for u in tweets.includes['users']}
-- MAGIC 
-- MAGIC data = []
-- MAGIC 
-- MAGIC for tweet in tweets.data:
-- MAGIC     if users[tweet.author_id]:
-- MAGIC         user = users[tweet.author_id]
-- MAGIC         USERNAME = user.username
-- MAGIC         NAME     = user.name
-- MAGIC         TWITEE   = tweet.text
-- MAGIC         data.append([str('@'+USERNAME),  str(NAME), str(TWITEE)])
-- MAGIC         
-- MAGIC         df = spark.createDataFrame(data, ['USERNAME', 'NAME', 'TWITEE'])
-- MAGIC         
-- MAGIC df.show()
-- MAGIC 
-- MAGIC df.write.mode("overwrite").saveAsTable(Refined_Zone+".TWITTER_API_GB")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tabela Final Twitter

-- COMMAND ----------

SELECT
      *
FROM ${Refined_Zone}.TWITTER_API_GB

-- COMMAND ----------

-- DBTITLE 1,Tempo de execução PARTE 2
-- MAGIC %python
-- MAGIC final_parte_2 = datetime.now()
-- MAGIC duracao_parte_2 = final_parte_2 - inicio_parte_2
-- MAGIC print(duracao_parte_2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC FIM DO PROCESSO PARTE 2

-- COMMAND ----------

-- DBTITLE 1,Tempo total de execução do processo
-- MAGIC %python
-- MAGIC final = datetime.now()
-- MAGIC duracao_total = final - inicio
-- MAGIC print(duracao_total)
