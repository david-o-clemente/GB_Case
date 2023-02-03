
Armazenamento de scripts dos cases efetuados na reposta dos cases do GB

# GB_Case2
Para o desenvolvimento do Case2, foi usado o databricks community.

Foi criado o notebook: Case2_GB - Final
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4099409315154074/2354220083622024/193667195265007/latest.html

1º: Foi criado o cluster *Culster_GB*, com a configuração Databricks 11.3 LTS (Spark 3.3.0, Scala 2.12).

<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216492466-70338596-a91d-4b94-b775-cd944fea1490.png" width="500px" />
</div>


1.1º: Instalar a Libraries **_com.crealytics:spark-excel_2.12:0.13.6_** para fazer a leitura de arquivos xls (excel) pelo spark.
     1ºPasso: Clicar no icone *install new*
     2ºPasso: Na categoria *Library Source* clicar na opção **Maven**
     3ºPasso: Na opção Coordinates colar a descrição **com.crealytics:spark-excel_2.12:0.13.6**

