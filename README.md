
# Documentação e scripts
Documentação explicativa da resolução do case + scripts usados.<br />

#### Breve explicação da Arquitetura usada para a solução do Case2:  Conforme a leitura do Case2 entendi que poderia aplicar a arquitetura medallion, pois se tratava de uma processo em camadas, o que vai de encontro com a ideia da arquitetura medallion conforme o próprio glossário da Databricks:

*_"Uma arquitetura medallion é um padrão de projeto de dados usado para organizar logicamente os dados em uma lakehouse, com o objetivo de melhorar de forma incremental e progressiva a estrutura e a qualidade dos dados à medida que fluem por cada camada da arquitetura (das tabelas de camadas Bronze ⇒ Silver ⇒ Gold) . As arquiteturas Medallion às vezes também são chamadas de arquiteturas 'multi-hop'"_*

Link para mais informações arquitetura medallion: https://www.databricks.com/glossary/medallion-architecture

<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216502895-0678ab16-5202-4757-8df9-5f3d955a3f29.png" width="500px" />
</div>

<br />
Após esta decisão da arquitetura e somando meu conhecimento atual na ferramenta Databricks, decidi que o desenvolvimento do Case2 será feito no Databricks community.

## Configuração Cluster

1º: Foi criado o cluster *Culster_GB*, com a configuração Databricks 11.3 LTS (Spark 3.3.0, Scala 2.12).
<br />
<br />
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216490358-de9e1317-8924-4f3c-b892-9e4f40a30873.png" width="500px" />
</div>

<br />
<br />


> Obs: Na versão Databricks community não é possivel fazer a leitura dos arquivos xlsx pelo pandas (Biblioteca mais comum para leitura e tratamento de arquivox xlsx). Para o desenvolvimento do Case2, foi feita a ingestão no DBSF de 3 arquivos xlsx, por isso foi necessário instalar uma libraries do spark que realiza leitura de arquivos xlsx.
<br />

1.1º: Instalação da Libraries **_com.crealytics:spark-excel_2.12:0.13.6_** para fazer a leitura de arquivos xlsx (excel) pelo spark.

* 1º Passo: Clicar no icone **install new**

* 2º Passo: Na categoria *Library Source* clicar na opção **Maven**

* 3º Passo: Na opção Coordinates colar a descrição **com.crealytics:spark-excel_2.12:0.13.6**
<br />

<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216492466-70338596-a91d-4b94-b775-cd944fea1490.png" width="500px" />
</div>

## Ingestão dos arquivos (lakehouse)

Fazendo a simulação do lakehouse, eu criei um diretório onde estariam todas os arquivos fornecidos pela origem, no cenário do Case2 são os três arquivos a serem consumidos.

<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216503922-aff83a0f-34c2-42f9-b93d-ccaac71be70f.png" width="500px" />
</div>

## Criação Notebook

Foi criado o notebook com nome Case2_GB_Final

link para o notebook:
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4099409315154074/2354220083622024/193667195265007/latest.html

Como Case2 se divide em duas partes, foi criado um indice na lateral esquerda do notebook para organizar toda a estrutura do código e facilitar o entendimento e a navegação no notebook.

<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216505511-b3abfa43-b6a2-4358-9eb0-be9a39ced5ac.png" width="200px" />
</div>

### Desbravando o Notebook

##### PARTE 1 

A PARTE 1 se divide em quatro partes, sendo elas:
* PARTE 1.1: Configuracao do ambiente: Aqui como boa prática é executado um notebook a parte onde ficam as configurações de ambiente, ou seja dentro do notebook Case2_GB_Final ele chama um outro notebook chamado Case2_GB_Config_Ambiente para setar coisas importantes no ambiente como:
  * Instalação da biblioteca **tweepy** que será usada na PARTE 2 para a interação com a API do Twitter.
  * Informações das Secret keys do Twitter.
  * Criação dos databases onde serão criadas as tabelas consumidas no script.
  * Variaveis que serão utilizadas no script.
  
* PARTE 1.2: Raw Data Zone (Bronze layer): Aqui é o primeiro nível do medallion, onde colocamos todos os dados de sistemas de origem externa, de forma "CRU", sem alterações nos dados, incluindo apenas informações que facilitam o uso nos demais niveis, como informação do sistema origem , data/hora carregamento e id_processo.
