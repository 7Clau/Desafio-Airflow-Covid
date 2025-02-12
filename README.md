# Informações sobre a base de dados fornecida

A base de dados que deverá ser tratada, encontra-se disponível neste mesmo respositório do GitLab na pasta: **/datalake/raw/covid19**

|Arquivo|Descrição|
|---|---|
|time_series_covid19_confirmed_global.csv|Contém informações sobre os casos **confirmados** do vírus|
|time_series_covid19_deaths_global.csv|Contém informações sobre as **mortes** confirmadas do vírus|
|time_series_covid19_recovered_global.csv|Contém informações sobre os casos de **recuperação** do vírus|

### Descrição das colunas
Cada arquivo mencionado anteriormente contém as mesmas colunas, seguem as descrições:

|Coluna|Descrição|
|---|---|
|Province/State|Estado ou Província|
|Country/Region|País ou Região|
|Lat|Latitude|
|Long|Longitude|
|Demais colunas|Contém as datas e os valores **acumulados** dos casos confirmados, as datas estão no formato MM/DD/YY|

# Software necessário

## Docker Desktop
O ambiente designado para desenvolver a solução foi pre-configurado com o Airfow e o Spark em uma imagem docker. 

Para executar este ambiente será necessário instalar em seu equipamento o Docker Desktop, disponível para download no seguinte link:

[Download Docker Desktop](https://www.docker.com/products/docker-desktop)

Após instalar o docker será possível subir o ambiente utilizando a seguinte linha de comando na raíz desse repositorio (onde encontra-se o arquivo `docker-compose.yaml`):

<pre><code>docker-compose up</code></pre>

Ao executar o comando acima, pode ser requerido a permissão de acesso aos seguintes diretórios:

- ./datalake
- ./dags
- ./logs
- ./plugins

Se estiver utilizando Windows uma mensagem pop-up como a seguinte pode aparecer multiplas vezes na primeira execução:

![Docker Permission](/_img/docker_permission.png?raw=true "Docker Permission")

Caso ocorra, basta clicar na opção: **Share it**.

## Apache Airflow
Ao executar o comando __docker-compose__ algumas mensagens de log aparecerão e o ambiente estará pronto para uso quando a saída do log estiver como na seguinte imagem:

![Docker Compose Logs](/_img/docker_compose_log.png?raw=true "Docker Compose Logs")

A interface visual do Apache Airflow estará acessível no seguinte endereço:

http://localhost:8080

E poderá ser acessada através das seguintes credenciais:

- login: **airflow**
- password: **airflow**

Uma dag de exemplo (sample.py) foi disponibilizada neste repositório para auxiliar no início do desenvolvimento do pipeline.

![Airflow Interface](/_img/airflow.png?raw=true "Airflow Interface")

## Apache Spark
O Apache Spark (versão 3.1.1) está disponível na imagem docker junto com o Airflow, você poderá acessá-lo através do Spark Session no Python, exemplo:
<pre><code>
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("airflow_app") \
    .config('spark.executor.memory', '6g') \
    .config('spark.driver.memory', '6g') \
    .config("spark.driver.maxResultSize", "1048MB") \
    .config("spark.port.maxRetries", "100") \
    .getOrCreate()
</code></pre>

# Instruções de Desenvolvimento

A solução deverá ser desenvolvida utilizando os diretórios deste repositório:

|Diretório|Descrição|
|---|---|
|datalake|Deverá conter os dados brutos e refinados da solução|
|dags|Deverá conter os códigos em Python (PySpark) desenvolvidos juntamente com a DAG do Airflow que será desenvolvida|


**Importante:** Os diretórios citados acima e presentes nesse repositório, foram pre-mapeados na imagem docker (via volumes no docker-compose), desta forma basta desenvolver o código localmente que o mesmo irá refletir diretamente no container do docker em execução.

Os arquivos de dados originais requeridos para esta solução já estão presentes no diretório:
<pre><code>./datalake/raw/covid19</code></pre>

Podendo ser acessados através do seguinte caminho no contâiner:
<pre><code>/home/airflow/datalake/raw/covid19</code></pre>

# Requesitos da solução
A solução desenvolvida deverá atender os seguintes requisitos:
## Pipeline
Deverá ser desenvolvida uma DAG no Apache Airflow com as seguintes características:
- Intervalo de execução diário
- Minimamente 2 tasks: ingestão >> processamento
- Utilização de PythonOperator nas tasks

## Processamento de Dados
O código da solução deverá ser desenvolvido utilizando o PySpark, podendo ser utilizadas as APIs: RDD, DataFrames ou Spark SQL.

### Camada Trusted
A solução deverá ser capaz de processar os dados contidos nos arquivos .CSV da pasta **datalake/raw/covid19**, efetuando uma unificação dos registros em uma única tabela e armazenando o seu resultado no diretório **datalake/trusted**.

A tabela desenvolvida nesta camada deverá atender a seguinte estrutura:
|Coluna|Descrição|Formato|
|---|---|---|
|pais|Deverá conter a descrição do País|string|
|estado|Deverá conter a descrição do Estado ou Província|string|
|latitude|Deverá conter coordenada geográfica de latitude|double|
|longitude|Deverá conter coordenada geográfica de longitude|double|
|data|Deverá conter a data do registro|timestamp|
|quantidade_confirmados|Deverá conter a quantidade de **novos** casos Confirmados na data específica|long|
|quantidade_mortes|Deverá conter a quantidade de **novas** Mortes na data específica|long|
|quantidade_recuperados|Deverá conter a quantidade de **novos** Recuperados na data específica|long|
|ano|Coluna de partitionamento que deverá conter o ano extraído da coluna data|int|
|mes|Coluna de partitionamento que deverá conter o mes extraído da coluna data|int|

### Camada Refined
A solução deverá ser capaz de processar os dados contidos na tabela anteriormente criada na camada **trusted**, efetuando uma agregação e cálculo das médias móveis dos 3 tipos de casos nos últimos 7 dias, armazenando o seu resultado no diretório **datalake/refined**.

A tabela desenvolvida nesta camada deverá atender a seguinte estrutura:
|Coluna|Descrição|Formato|
|---|---|---|
|pais|Deverá conter a descrição do País|string|
|data|Deverá conter a data do registro|timestamp|
|media_movel_confirmados|Deverá conter a média móvel dos últimos 7 dias de casos Confirmados até data específica|long|
|media_movel_mortes|Deverá conter a média móvel dos últimos 7 dias de Mortes até data específica|long|
|media_movel_recuperados|Deverá conter a média móvel dos últimos 7 dias de Recuperações até data específica|long|
|ano|Coluna de partitionamento que deverá conter o ano extraído da coluna data|int|

### Armazenamento de Dados
- Os dados nas camadas **trusted** e **refined** devem ser armazenados no formato **PARQUET**
- Os dados nas camadas **trusted** deverão estar particionados pelas colunas **ano e mes**
- Os dados nas camadas **refined** deverão estar particionados somente pela coluna **ano**
- Os dados nas camadas **trusted** e **refined** deverão conter apenas 1 arquivo **PARQUET** em cada Partição.

Fique livre para usar a sua criatividade e extrair novos insights! :smiley:

# Artefatos para entrega da solução

Para a entrega da solução, os seguintes artefatos deverão estar presentes no seu repositório:

- Código fonte desenvolvido na pasta **dags**.
- Arquivos **.parquet** das camadas **trusted** e **refined** na pasta **datalake**

Ao finalizar, envie um email para o o recrutador responsável pelo seu processo seguindo o padrão de assunto: "Prova 1 - [Nome] - [Tecnologia]".

Não esqueça de adicionar o usuário **radix.recruit** como reporter do seu repositório.

Crie ou edite o README do seu repositório para realizar comentários e/observações, por exemplo, o que achou dos desafios ou maiores dificuldades encontradas.

Bom desafio!!!