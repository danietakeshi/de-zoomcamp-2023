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