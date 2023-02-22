# Data Engineering Zoomcamp 2023 Files

Repositório aonde ficaram armazenados os arquivos utilizados durante o Data Engineering Zoomcamp.

Abaixo ficaram os posts que serão publicados ao fechamento de cada semana no LinkedIn.

---

## Week 1

Estou participando do Data Engineering Zoomcamp do DataTalksClub.

Tópicos da primeira semana:
- Introdução ao Docker e criação de um pipeline de dados;
- Utilizando um container do Postgres e fazendo carregamento de dados;
- Utilizando pgAdmin para acessar as informações do Banco de Dados (Docker networks);
- Rodando um script de ingestão de dados em Python dentro de um container;
- Fazendo uso do docker-compose para configuração de múltiplos containers;
- Conceitos básicos de SQL (basic data quality checks, Inner joins, Group by ...);
- Criação de um ambiente no GCP (Google Cloud Platform) com o auxílio do Terraform;

Anotações, arquivos utilizados e atividades estão em um repositório do [GitHub](https://github.com/danietakeshi/de-zoomcamp-2023).

[#dezoomcamp](https://www.linkedin.com/posts/daniel-takeshi-martins-a62259aa_python-sql-dataengineering-activity-7024192540534841344-FYmU?utm_source=share&utm_medium=member_desktop)

---

## Week 2

E ai?! Tem Dado no Trabalho?

Continuando aqui minha saga de Dados no Zoomcamp de Data Engineering que apresenta diversas ferramentas úteis para quem precisa trabalhar com dados.

Nesta semana (Week 2) foi possível aprender como orquestrar fluxos de trabalho utilizando o Prefect (eu não escrevi errado, o nome da ferramenta é esse mesmo: [Prefect](https://www.prefect.io/)).

O Prefect facilita o monitoramento de Pipelines de Dados, fazendo possível o agendamento de processos de ETL, reprocessamento em caso de falha, registro de logs, utilização de cache, envio de notificações dentre outras funcionalidades. Tudo isso com uma UI amigável e com a possibilidade de usar este serviço na cloud.

O principal aprendizado desta semana foi a criação de um fluxo de dados automatizado com Python e Prefect, utilizando parâmetros para maior flexibilidade, que extrai arquivos da web, faz os tratamentos necessários e depois insere estes dados no Google Big Query. Se este processo falhar, uma notificação automática é enviada para um canal no Slack.

Então se você tem dado no trabalho, dê uma olhada nesta ferramenta.

Anotações, arquivos utilizados e atividades estão no meu repositório do [GitHub](https://github.com/danietakeshi/de-zoomcamp-2023).

#dezoomcamp #week2 #dadonotrabalho

---

## Week 3

Resumo da semana 3 da jornada de Data Engineering...

Criação de um Data Warehouse no BigQuery.

Esta ferramenta possui alguns benefícios como escalabilidade e alta disponibilidade possuindo serviços integrados de Machine Learning, análise Geoespacial e de Bussiness Intelligence.

Entretanto, estes benefícios tem um custo, sendo necessário o planejamento e a otimização das consultas tendo em vista o aspecto financeiro.

Dentro destas otimizações temos:
- Partitioning: Cria partições baseadas em uma coluna temporal, por exemplo, agrupamento pelo dia de ingestão dos dados no banco.
- Clustering: Faz o organização de dados correlacionados e otimiza as consultas que utilização o filtros nas colunas que possuem um cluster.

Como exemplo de aplicações e quantificação dos benefícios destas otimizações, fizemos um exercícios de comparação entre as consultas com e sem a utilização do Partitioning e de Clustering. Sem o uso tivemos um total de 647MB de dados processados, e, após a aplicação, foram processados 23MB.

Por este motivo a otimização das consultas é fundamental, principalmente quando estamos utilizando um banco de dados na nuvem, em que o custo está diretamente ligado na quantidade de dados processados.

Anotações, arquivos utilizados e atividades estão no meu repositório do GitHub. (https://github.com/danietakeshi/de-zoomcamp-2023)

#dezoomcamp #week3 #bigquery #otimização

---

## Week 4

Semana 4 da jornada de Data Engineering...

Utilizando o dbt (data build tool) para efetuar o processo de transformação de dados (criação de tabelas e views) em um DataLake no BigQuery.

Dentro de um processo de ELT (Extract, Load and Transformation) o dbt será o responsável pela manipulação dos dados para a criação de uma tabela pronta para o consumo em um programa de visualização como o Google Data Studio.

Uma das suas vantagens é a integração com o Github facilitando o versionamento dos modelos criados, além disso possui ferramentas para testes e documentação. Mostra de maneira visual as integrações das tabelas e views.

Com os dados prontos, é só conectar a base de dados em um software de visualização para extrair informações relevantes.

Anotações, arquivos utilizados e atividades estão no meu repositório do GitHub. (https://github.com/danietakeshi/de-zoomcamp-2023)

#dezoomcamp #week4 #dbt #dataengineering #datastudio