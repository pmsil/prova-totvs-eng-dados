# Projetos para Resolver a prova de Engenharia de Dados TOTVS
![Badge em Desenvolvimento](http://img.shields.io/static/v1?label=STATUS&message=PRONTO&color=GREEN&style=for-the-badge)
## Este repositório contém três projetos desenvolvidos para resolver problemas específicos utilizando diferentes tecnologias e abordagens.
### OBS: Todos os projetos foram testados e estão com arquivos que permitem a execução dos testes.

## Projeto 1: Pipeline com Apache Spark em Java (Projeto: prova_TOTVS)

1. Implemente em Java o seguinte pipeline usando Apache
Spark:

- Lê um arquivo de logs que possui duas colunas separadas por tab. Na primeira coluna, os valores podem ser 1 ou 0. A segunda coluna é formada por uma
lista de palavras.

- Divida o log em duas partes: uma com as linhas começando com 0 e outra com as linhas começando com 1
- Faça um processador para contar quantas linhas começam com 0
- Faça um processador para contar as palavras da segunda coluna das linhas que começam com 1
- Faça um processador para contar as palavras únicas da segunda coluna das linhas que começam com 1
- Grave os resultados em arquivo texto


O projeto utiliza Java, Spark e foi criado na IDE IntelliJ.
Para isso foi necessário configurar a conexão entre o Spark e o Java (no IntelliJ).

## Projeto 2: Consulta MongoDB com JavaScript (Projeto: consulta-javascript-mongoDB)

Descrição do projeto 2.

Imagine uma coleção no MongoDB que contenha as colunas:
id_nota, produto, categoria, departamento, valor.
Implemente uma consulta para retornar o valor médio por categoria de um departamento específico. 
Envie o código javascript do MongoDB. Não é necessário fazer um programa em Java.

Esse projeto foi criado no Visual Studio utilizando Node.js e o MongoDB.
No código é criada uma conexão no MongoDB e foi criada uma função que executa a solicitação da questão.

## Projeto 3: Pipeline com Apache Spark para Gerenciamento de Vendas em Java

Descrição do projeto 3. (Projeto: pipeline_gerencia_produtos)

Implemente em Java um pipeline utilizando Apache Spark para resolver o seguinte caso de uso: 
Um departamento recebe informações de vendas de produtos através de um arquivo CSV contendo o código do produto, nome do produto,
data da venda, valor da venda e o ID do comprador. O pipeline precisa lidar com os seguintes requisitos:

a. Registrar novos produtos: sempre que um novo produto for identificado, ele deve ser registrado. Isso envolve
incluir o código do produto e o nome do produto em uma coleção no MongoDB.

b. Gerar um relatório CSV contendo o número de vendas e o número de compradores dos novos produto antes do
registro ser feito.

c. Ignorar produtos que já estão cadastrados, pois eles já são processados por outro pipeline de dados.

O projeto utiliza Java (com Maven), Spark, MongoDB e foi criado na IDE IntelliJ.
Para isso foi necessário configurar a conexão entre o Spark e o Java (no IntelliJ) e também a utilização de conectores
para que o Spark se conecta-se no MongoDB (configurei o arquivo pom.xml para que buscasse as dependências).
