
# Documentação e scripts
Documentação explicativa da resolução do Case2 + scripts usados.<br />

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
<img src="https://user-images.githubusercontent.com/98194507/216828113-2fad702d-df6b-4932-98f0-291788520baf.png" width="950px" />
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
</div>

## Execução Notebook principal

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
