
# Documentação
Documentação explicativa da resolução do Case2.<br />

#### Breve explicação da Arquitetura usada para a solução do Case2:  Conforme a leitura do Case2 entendi que poderia aplicar a leitura de arquivos no data lake usando a arquitetura medallion, pois se tratava de uma processo em camadas, usando o processo de ETL, o que vai de encontro com a ideia da arquitetura medallion conforme o próprio glossário da Databricks:

*_"Uma arquitetura medallion é um padrão de projeto de dados usado para organizar logicamente os dados em uma data lake, com o objetivo de melhorar de forma incremental e progressiva a estrutura e a qualidade dos dados à medida que fluem por cada camada da arquitetura (das tabelas de camadas Bronze ⇒ Silver ⇒ Gold) . As arquiteturas Medallion às vezes também são chamadas de arquiteturas 'multi-hop'"_*

Link para mais informações arquitetura medallion: https://www.databricks.com/glossary/medallion-architecture

<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216502895-0678ab16-5202-4757-8df9-5f3d955a3f29.png" width="500px" />
</div>

<br />
Após esta decisão da arquitetura e somando meu conhecimento atual na ferramenta Azure Databricks, escolhi esta plataforma para a resolução do case.

## Configuração Cluster

1º: Foi criado o cluster *DAVID CLEMENTE's Personal Compute Cluster*, com a configuração Databricks 11.3 LTS (Spark 3.3.0, Scala 2.12).
<br />
<br />
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216824824-9bbaf946-66e7-479c-865e-0e49e6ae9606.png" width="950px" />
</div>

<br />
<br />

## Ingestão dos arquivos (Data Lake)

#### PARTE 1

Simulando uma esteira de produção, na parte da ingestão foi criado um Data Lake Storage Para alocação dos arquivos que serão usados na ingestão, segue os passos da criação:
<br />
<br />
1.1. **Storage accounts**: Criação da conta de armazenamento com nome stgbcas2.
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216827720-7ba50110-e06b-41b0-afb5-7b692eebb022.png" width="9500px" />
</div>
<br />

1.2. **Data Lake Storage**: Com a conta de armazenament criada, a opção Data Lake Storage ficou disponivel.
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216860571-7fb1776f-5153-41fd-8692-963abb0cf209.png" width="950px" />
</div>
<br />

1.2.1 **Containers**: Clicando em Data Lake Storage temos a opção de criar Containers, os arquivos do Case2 foram recebidos por email, simulei que o sistema origem seria email, então criei um container com o nome ctdadosgbcase2email.
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216828226-8b071e9b-ac12-4778-bed4-441c97bbf3e7.png" width="950px" />
</div>
<br />

1.2.2 **Container ctdadosgbcase2email**: Com o container ctdadosgbcase2email criado, foi feito os upload dos arquivos recebidos.
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216828677-bdf47633-9bbe-4eae-ab14-968d1078a352.png" width="950px" />
</div>
<br />

#### PARTE 2

Com o Data Lake criado, agora é preciso criar o ponto de montagem no databricks para acessar os arquivos da origem email.<br />
Criei um notebook chamado **_Case2_GB_Mount_DataLake_** para deixar concentrado todas as informações desta etapa.

1º Passo: Criar a pasta no dbfs para criar o ponto de montagem dos arquivos do delta lake, neste caso foi criado um diretorio chamado GB e dentro dele uma pasta para cada origem, neste caso email.
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216831077-0f02b501-49b0-4fc8-8345-abda9b4f2fdb.png" width="500px" />
</div>
<br />

> Como ainda não fizemos o ponto de montagem, os arquivos ainda não estarão disponiveis:

2º Passo: Criar o comando de montagem, neste caso eu fiz de forma parametrizada, colocado os valores em variaveis, percebe-se que agora ao realizar uma nova consulta os arquivos já irão aparecer no diretorio criado anteriormente.

<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216831539-c887e8e8-35d1-4c39-a6ac-a297d5837819.png" width="800px" />
</div>
<br />

> Obs: Não é necessário criar um ponto de montagem a cada ingestão de arquivos no Delta lake, o diretório é atualizado automaticamente. 

Link para mais informações sobre ponto de montagem: https://learn.microsoft.com/pt-br/azure/databricks/dbfs/mounts
<br />

## Execução Notebook Principal

Foi criado o notebook com nome Case2_GB_Final

Como Case2 se divide em duas partes, foi criado um indice na lateral esquerda do notebook para organizar toda a estrutura do código e facilitar o entendimento e a navegação no notebook.

<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216832315-4151acec-5890-42d5-ab42-6fd2b691e218.png" width="400px" />
</div>

### Desbravando o Notebook
##### PARTE 1 
A PARTE 1 se divide em quatro partes, sendo elas:
<br />
* PARTE 1.1: **Configuracao do ambiente**: Aqui como boa prática é executado um notebook a parte onde ficam as configurações de ambiente, ou seja dentro do notebook Case2_GB_Final ele chama um outro notebook chamado Case2_GB_Config_Ambiente para setar coisas importantes no ambiente como:
  * Instalação da biblioteca **openpyxl** serve para ler aquivos XLXS.
  * Instalação da biblioteca **tweepy** que será usada na PARTE 2 para a interação com a API do Twitter.
  * Informações das Secret keys do Twitter.
  * Criação dos databases onde serão criadas as tabelas consumidas no script.
  * Variaveis que serão utilizadas no script.

* PARTE 1.2: **Raw Data Zone (Bronze layer)**

   Aqui é o primeiro nível da arquitetura medallion, onde é realizado a extração dos arquivos do data lake,de forma "CRU", sem alterações nos data types, nesta etapa foi utilizado desenvolvimento mesclando python com algumas bibliotecas e pyspark para criação da tabela hive database **BRONZE**, podemos dividir esta parte em algumas operações:
   * Em **PYTHON**, a criação de uma operação de loop olhando para os arquivos do diretorio **("/mnt/gb/email")** para concatenar todos os arquivos em um único arquivo para ser consumido na extração.
   * Em **PYTHON (PANDAS)**, fazendo o carregamento de todas as colunas em formato **STRING** para não perder dados e saber como está vindo da origem.
   * Criação de colunas que ajudam no controle da extração e facilitam o uso nos demais niveis:
     * Informação do sistema origem **(SISTEMA_ORIG)**
     * Tipo do formato do arquivo origem **(TIPO_ARQV_ORIG)**
     * Data e a hora do processando da extração **(DT_HR_PROC)**
     * Chave com a contatenação de todas as colunas originais do arquivo **(CHAVE_EXTRACT_BASES)**
   * Em **PYSPARK**, a criação da tabela  no hive database **BRONZE.BASE**.<br /> 
   * Conversão dos campos que se repetem a descrição para o formato **CATEGORY**, otimizando a leitura.<br /> 
      Campos 'MARCA', 'LINHA', 'QTD_VENDA','SISTEMA_ORIG','TIPO_ARQV_ORIG'.<br/>
<br />      
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216845060-384fed7b-e539-494a-b390-f15ca0909083.png" width="300px" />
</div><br/>

> Segue a análise do comparativo, onde demonstro o resultado sem os campos no formato **CATEGORY** e o resultado na conversão de cada campo, percebe-se uma uma redução significativa em termos de consumo de memoria, cerca de 57% de redução aproximadamente.<br /> 

<br/>
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216845437-056887f3-aca1-4cf9-ad44-81646251a88e.png" width="800px" />
</div><br/>

Demonstração da tabela:
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216839905-ed54d356-559c-4019-8471-09a6a01de2e9.png" width="1500px" />
</div>

Demonstração do código:
<br/>
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216846252-3d464ae8-3797-4e2a-a65c-beed194d8bac.png" width="800px" />
</div><br/>

* PARTE 1.3: **Trusted Zone (Camada Silver)**: Nesta etapa é feita o DDL da tabela **SILVER.BASE** e também é realizado tratamentos na tabela gerada na etapa anteior **(Raw Data Zone (Bronze layer))**, realizando também duas validações:<br/>
  * Validação 01: Removendo os dados duplicados.<br/>
    > Nos arquivos disponibilizados, temos dois arquivos com dados iguais porém o nome do arquivo é diferente (Base 2017 / Base_2019(2)).<br/>
  * Validação 02: São inseridos na tabela os registros que a chave não consta na tabela **SILVER.BASE**.<br/>
Após as validações, é feito o insert no hive database **SILVER.BASE**.<br/>

Query que efetua a validação:
<br/>
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216849482-3802f90b-c002-4def-9534-539d45174834.png" width="800px" />
</div><br/>

Demonstração que existe registros repetidos:<br/>
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216854763-bd5cd937-a38e-4993-90ae-de02905d8f6c.png" width="1000px" />
</div><br/>

Query com os counts das duas tabelas BRONZE.BASE / SILVER.BASE
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216849818-f37554fc-1e21-447d-905c-36c453c479db.png" width="500px" />
</div><br/>

* PARTE 1.4: **Refined Zone (Camada Gold)**: Nesta etapa é onde os dados estão refinados e estão disponível para o consumo da área de negócio, foram criadas tabelas com analogia aos questionamentos do case fazendo.
  * a. Tabela1: Consolidado de vendas por ano e mês;<br/>
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216855199-ad906620-a58b-4ec5-8316-a1b5d7a5510c.png" width="400px" />
</div>
<br/>
  * b. Consolidado de vendas por marca e linha;<br/>
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216855247-79b13d43-f379-45ff-a211-4bc24e148dc1.png" width="400px" />
</div>
<br/>
  * c. Tabela3: Consolidado de vendas por marca, ano e mês;<br/>
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216855260-46b90bbc-5fbc-4ee9-9046-ba632b94d95b.png" width="400px" />
</div>
<br/>
  * d. Tabela4: Consolidado de vendas por linha, ano e mês;<br/>
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216855286-594e1dc5-0c99-4361-abd6-33d5d2c280c1.png" width="400px" />
</div>

##### PARTE 2

A PARTE 2 se divide em duas partes, sendo elas:
<br />

* PARTE 2.1: **Consulta API Twitter**: Nesta parte foi criado uma conexão com a API do Twitter com as chaves secretas de desenvolver do twitter, usando as variáveis definidas no notebook Case2_GB_Config_Ambiente para a definição do função **cliente** que é obrigátorio para o acesso da API, também foi definido o critério de pesquisa e quais informações queremos dos tweets, segue algumas informações:

Definção da função cliente de forma parametrizada:  

<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216856754-faeeb9cc-4ecc-4882-a343-8f0c2644838f.png" width="800px" />
</div>

Critérios de palavras chaves dos tweets: 

<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216856696-ca6a47db-4511-45b2-8683-7bae035e6a05.png" width="400px" />
</div>

Definição dos parametros da função cliente, para trazer informações necessárias para resolução do Case2: 
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216858236-d8afc8c9-9e99-4e80-b3e3-f3040f31343d.png" width="1200px" />
</div>

Definição da varivavel users para armazernar os ID das pessoas responsaveis pelo tweet: 
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216858790-09c10f28-42b1-47ba-a75b-8540c8ffa14d.png" width="500px" />
</div>

Criação uma operação de loop para pegar as variáveis definidas em relação a cada ID e criar colunas com cada valor, carregando todas as informações na variavel **data**: 
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216858913-2a10e3db-3f82-47f4-ba2d-e6d7029fd6c3.png" width="500px" />
</div>

Criação da tabela  **GOLD.TWITTER_API_GB** usando os dados da variavel **data**:
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216859350-7b7dd9cd-ae26-4149-ad32-271fd9532968.png" width="500px" />
</div>

> API do Twitter só mostra os resultados da consulta de até 7 dias anterioes..<br />
> Com isso resultados sofrem alterações dependendo da data de execução.<br />

* PARTE 2.2: **Tabela Final Twitter**

Commit via Databricks
<br />
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216859621-a176eddf-fbe3-410d-aa5f-3f841cb3fd3a.png" width="1000px" />
</div>
<br />

> É possivel obter o tempo de processamento de cada etapa do script, no final de cada parte tem a variavel **duracao_total**.<br />
> Segue tempo atual de execução total do script principal:<br />
<br />
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/216859948-26cb5c8b-b4b2-47a6-8a26-49c7f126cac7.png" width="1000px" />
</div>

Segue imagem da arquitetura de solução do Case2:<br />
<div align="center">
<img src="https://user-images.githubusercontent.com/98194507/217128779-9f34d57d-00a8-43fc-a4c3-26a23d5d45c1.png" width="1000px" />
</div>



##### FIM DO PROCESSO.
