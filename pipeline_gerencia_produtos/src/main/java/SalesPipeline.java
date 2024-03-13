import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import java.util.List;


public class SalesPipeline {
    private final SparkSession sparkSession;

    public SalesPipeline() {
        this.sparkSession = SparkSession.builder()
                .appName("SalesPipeline")
                .master("local")
                .config("spark.mongodb.read.uri", "mongodb://localhost/test.myCollection")
                .config("spark.mongodb.write.uri", "mongodb://localhost/test.myCollection")
                .getOrCreate();
    }


    public Dataset<Row> readCSV(String filePath) {
        return sparkSession.read()
                .option("header", "true")
                .option("delimiter", ";") // Especificar o delimitador correto
                .option("inferSchema", "true") // Inferir automaticamente o esquema das colunas
                .csv(filePath);
    }
/*
    public void processSales (Dataset<Row> salesData, MongoDBConnector mongoConnector)  {
        // Dataset<Row> productsInMongoDB = (Dataset<Row>) mongoConnector.getCollection( "produtos");

        Dataset<Row> loadedDf = sparkSession.read()
                .format("mongodb")
                .option("database", "totvs")
                .option("collection", "produtos")
                .load();

        // Exibindo os dados lidos
        loadedDf.show();
        loadedDf.createOrReplaceTempView("characters");

        // Verificar se o produto já existe na coleção "totvs.produtos" do MongoDB
        salesData.foreachPartition(rows -> {
            while (rows.hasNext()) {
                Row row = rows.next();

                String cod_produto = row.getString(0); // Supondo que a primeira coluna seja o código do produto
                String nome_produto = row.getString(1); // Supondo que a segunda coluna seja o nome do produto

               // boolean existsInMongoDB = productsInMongoDB.filter(productsInMongoDB.col("cod_produto").equalTo(cod_produto)).count() > 0;
                Dataset<Row> centenarians = sparkSession.sql("SELECT cod_produto, nome_produto FROM characters WHERE cod_produto = " + cod_produto);


                boolean existsInMongoDB = centenarians.count() > 0;
                if (!existsInMongoDB) {
                    // Produto não existe no MongoDB, então criamos o registro no MongoDB e no relatório CSV
                    mongoConnector.getCollection("produtos").insertOne((Document) row);

                    // Criar um novo DataFrame com os dados necessários para o relatório
                    Dataset<Row> reportData = sparkSession.createDataFrame(Arrays.asList(row), salesData.schema());

                    // Escrever o relatório CSV
                    reportData.write().mode(SaveMode.Append).csv("relatorio_produtos.csv");
                } else {
                    // Produto já existe no MongoDB, então gravamos o registro no relatório "ja_existentes.csv"
                    Dataset<Row> reportData = sparkSession.createDataFrame(Arrays.asList(row), salesData.schema());
                    reportData.write().mode(SaveMode.Append).csv("ja_existentes.csv");
                }
            }
        });
    }
*/


    public void processSales(Dataset<Row> salesData, MongoDBConnector mongoConnector) {
/*
        Dataset<Row> newProductsReport = salesData.groupBy("cod_produto")
                .agg(
                        functions.count("cod_produto").alias("num_vendas"),
                        functions.countDistinct("id_comprador").alias("num_compradores")
                );

        // Write the report CSV
        newProductsReport.write().mode(SaveMode.Overwrite).csv("relatorio_vendas.csv");
*/
        List<Row> rowsList = salesData.collectAsList();
        for (Row row : rowsList) {
            processPartition(salesData, row);
        }
    }
    private void processPartition(Dataset<Row> salesData,Row row) {

        Dataset<Row> loadedDf = sparkSession.read()
                .format("mongodb")
                .option("database", "totvs")
                .option("collection", "produtos")
                .load();

        // Exibindo os dados lidos
        loadedDf.show();
        loadedDf.createOrReplaceTempView("characters");




        String line = row.toString();
        // Divide a linha usando o ponto e vírgula como delimitador
        String[] values = line.split(",");


        /*
        String cod_produto = values[0].trim();
        String nome_produto = values[1].trim();
        String data_venda = values[2].trim();
        int valor_venda = Integer.parseInt(values[3].trim());
        String id_comprador = values[4].trim();
        */
        String cod_produto = values[0].trim().replace("[","");
        String nome_produto = values[1].trim();

        Dataset<Row> centenarians = sparkSession.sql("SELECT cod_produto, nome_produto FROM characters WHERE cod_produto = " + cod_produto);
        boolean existsInMongoDB = centenarians.count() > 0;

        /*
        List<Row> data = List.of(
                RowFactory.create(cod_produto, nome_produto, data_venda, valor_venda, id_comprador ));
        */

        List<Row> data = List.of(
                RowFactory.create(cod_produto, nome_produto));
        /*
        // Definindo o schema para o DataFrame
        StructType schema = new StructType()
                .add("cod_produto", DataTypes.StringType)
                .add("nome_produto", DataTypes.StringType)
                .add("data_venda", DataTypes.StringType)
                .add("valor_venda", DataTypes.IntegerType)
                .add("id_comprador", DataTypes.StringType);
        */

        // Definindo o schema para o DataFrame
        StructType schema = new StructType()
                .add("cod_produto", DataTypes.StringType)
                .add("nome_produto", DataTypes.StringType);

        // Criando o DataFrame
        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        if (!existsInMongoDB) {
            // Produto não existe no MongoDB, então criamos o registro no MongoDB e no relatório CSV

            // Realizar a agregação apenas para o cod_produto atual
            Dataset<Row> newProductsReport = salesData.filter(salesData.col("cod_produto").equalTo(cod_produto))
                    .groupBy("cod_produto")
                    .agg(
                            functions.count("cod_produto").alias("num_vendas"),
                            functions.countDistinct("id_comprador").alias("num_compradores")
                    );

            // Escrever o relatório CSV para o cod_produto atual
            newProductsReport.write().mode(SaveMode.Append).option("header", "true").csv("relatorio_" + cod_produto + ".csv");
            //mongoConnector.getCollection("totvs", "produtos").insertOne(row);

            // Escrevendo os dados no MongoDB
            df.write().format("mongodb").mode("append").option("database", "totvs")
                    .option("collection", "produtos").save();

            // Criar um novo DataFrame com os dados necessários para o relatório
            //Dataset<Row> reportData = sparkSession.createDataFrame(data, schema);

            // Escrever o relatório CSV
            // df.write().mode(SaveMode.Append).option("header", "true").csv("relatorio_produtos.csv");
        } else {
            // Produto já existe no MongoDB, então gravamos o registro no relatório "ja_existentes.csv"
            // Dataset<Row> reportData = sparkSession.createDataFrame(Arrays.asList(row), salesData.schema());
            df.write().mode(SaveMode.Append).option("header", "true").csv("ja_existentes.csv");
            // reportData.write().mode(SaveMode.Append).csv("ja_existentes.csv");
        }
    }

}
