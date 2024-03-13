const { MongoClient } = require('mongodb');

// Definindo a URL de conexão do MongoDB
const url = 'mongodb://localhost:27017';

// Nome do banco de dados e da coleção
const dbName = 'meuBancoDeDados';
const collectionName = 'minhaColecao';

// Função principal para executar a operação
async function main() {
  // Criando um cliente MongoDB
  const client = new MongoClient(url);

  try {
    // Conectando ao servidor MongoDB
    await client.connect();
    console.log('Conectado ao servidor MongoDB');

    // Selecionando o banco de dados
    const db = client.db(dbName);

    // Inserindo os dados de exemplo na coleção
    await inserirDados(db);

    // Realizando a consulta para obter o valor médio por categoria de um departamento específico
    await consultarValorMedioPorCategoria(db, 'Tecnologia');
  } catch (error) {
    console.error('Ocorreu um erro:', error);
  } finally {
    // Fechando a conexão com o cliente MongoDB
    await client.close();
    console.log('Conexão fechada');
  }
}

// Função para inserir os dados de exemplo na coleção
async function inserirDados(db) {
  const collection = db.collection(collectionName);
  const dados = [
    { id_nota: 10, produto: 'A', categoria: 1, departamento: 'Tecnologia', valor: 100 },
    { id_nota: 5, produto: 'B', categoria: 1, departamento: 'Saúde', valor: 200 },
    { id_nota: 1, produto: 'C', categoria: 1, departamento: 'Tecnologia', valor: 50 },
    { id_nota: 5, produto: 'D', categoria: 2, departamento: 'Tecnologia', valor: 100 },
    { id_nota: 5, produto: 'E', categoria: 3, departamento: 'Tecnologia', valor: 10 },
    { id_nota: 6, produto: 'F', categoria: 3, departamento: 'Tecnologia', valor: 20 }
  ];
  await collection.insertMany(dados);
  console.log('Dados inseridos com sucesso');
}

// Função para consultar o valor médio por categoria de um departamento específico
async function consultarValorMedioPorCategoria(db, departamento) {
  const collection = db.collection(collectionName);
  const pipeline = [
    { $match: { departamento: departamento } },
    { $group: { _id: "$categoria", valorMedio: { $avg: "$valor" } } }
  ];
  const result = await collection.aggregate(pipeline).toArray();
  console.log('Valor médio por categoria de', departamento + ':');
  console.log(result);
}

// Executando a função principal
main();
