import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class SparkMongoDBConnection {
    public static void main(String[] args) {
        // Configurando a sessão do Spark
        SparkSession spark = SparkSession.builder()
                .appName("SparkMongoDBConnection")
                .master("local") // ou qualquer URL do mestre do Spark que você esteja usando
                .config("spark.mongodb.read.uri", "mongodb://localhost/test.myCollection")
                .config("spark.mongodb.write.uri", "mongodb://localhost/test.myCollection")
                .getOrCreate();

        // Criando uma lista com alguns dados de exemplo
        List<Row> data = Arrays.asList(
                RowFactory.create("001","Product A","10/03/2024",100, "1"),
                RowFactory.create("002","Product B","11/03/2024",150, "1")

        );

        // Definindo o schema para o DataFrame
        StructType schema = new StructType()
                .add("cod_produto", DataTypes.StringType)
                .add("nome_produto", DataTypes.StringType)
                .add("data_venda", DataTypes.StringType)
                .add("valor_venda", DataTypes.IntegerType)
                .add("id_comprador", DataTypes.StringType);


        // Criando o DataFrame
        Dataset<Row> df = spark.createDataFrame(data, schema);

        // Escrevendo os dados no MongoDB
        df.write().format("mongodb").mode("overwrite").option("database", "totvs")
                .option("collection", "produtos").save();

        // Lendo os dados de volta do MongoDB
        Dataset<Row> loadedDf = spark.read()
                .format("mongodb")
                .option("database", "totvs")
                .option("collection", "produtos")
                .load();

        // Exibindo os dados lidos
        loadedDf.show();


       // implicitDS.createOrReplaceTempView("characters");
        loadedDf.createOrReplaceTempView("characters");
        Dataset<Row> centenarians = spark.sql("SELECT cod_produto, nome_produto FROM characters WHERE valor_venda = 150");
        centenarians.show();


        String cod_produto = new String();
        cod_produto = "001";

        Dataset<Row> centenarians3 = spark.sql("SELECT cod_produto, nome_produto FROM characters WHERE cod_produto = " + cod_produto);
        centenarians3.show();

        // Parando a sessão do Spark
        spark.stop();
    }
}
