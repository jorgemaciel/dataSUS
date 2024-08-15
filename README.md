## Dados do Sistema de Informação Hospitalar (SIH) Fortaleza Ceará

Este projeto consiste em entregar uma datalake com dados do Sistema de Informação Hospitalar (SIH) do DATASUS usando as ferramentas:
- Airflow (2.7.1)
- PySpark (3.5.1)
- MinIO (AGPL v3) :flamingo:
- Jupyter Lab

<p align="center">
<img src="assets/datalake.drawio.png" width=100% height=60%>

### Funcionalidades:

✅ Pipeline para ingestão de dados no formato dbc e transformados em csv na camada raw\
❓ Processar dados da camada row para a bronze\
❓ Processar dados da camada row para a silver\
❓ Dashboard

### Objetivo:

Criar um ambiente centralizado e estruturado para armazenar e processar os dados do SIH, permitindo análises e insights.

# :card_index_dividers:	Dataset's
Os dados brutos são armazenados na camada raw do MinIO :flamingo:
- :white_check_mark: Record Linkage Comparison Patterns https://bit.ly/1Aoywaq

### Build e start containers
Primeiro, você precisa construir uma imagem docker digitando `make build`. Depois disso, digite `make start` toda vez que quiser iniciar o serviço.

### Usando Jupyter
Após a conclusão do processo de construção e inicialização, digite `make token` e copie o resultado.

Acesse [http://localhost:8888](http://localhost:8888), cole o token no campo text/password e envie. Se tudo estiver certo, agora você tem acesso ao Jupyter Lab e pode criar scripts python normalmente.

### Acessando Airflow
Acesse [http://localhost:9090](http://localhost:9090) Usando o Apache Airflow como orquestrador para a ingestão de dados do nosso Data Lake.
- username: admin
- passsword: admin

### Acesse MinIO
Acesse [http://localhost:9000](http://localhost:9000) e faça login usando estas credenciais:
- username: minioadmin
- passsword: minioadmin

Agora você pode criar seus próprios buckets para salvar e manipular arquivos como um AWS S3 :wine_glass:.

### Acessando Spark Web UI
Acesse [http://localhost:8080](http://localhost:8080) para inspecionar aplicativos e workers do PySpark (por padrão, o `docker-compose.yml` é configurado para executar 1 worker do PySpark com 1 vCore e 1 GB de memória cada).

Para inspecionar os estágios de execução, você pode acessar [http://localhost:4040](http://localhost:4040) durante a execução.

### Stop containers
Para parar todos os contêineres, digite `make stop` no terminal e espere que todos eles sejam baixados.

## :package: Volumes
Os exemplos estão no diretório `workspace/` na raiz do projeto. Esta pasta é compartilhada entre a máquina host e o jupyter workspace em execução dentro do contêiner.