import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Main {
    public static void main(String[] args) {
        // Conectar ao MongoDB
        MongoDBConnector mongoConnector = new MongoDBConnector("mongodb://localhost:27017", "sales");

        // Criar o pipeline de vendas
        SalesPipeline salesPipeline = new SalesPipeline();

        // Ler o arquivo CSV de vendas
        String filePath = "vendas_produtos.csv";
        Dataset<Row> salesData = salesPipeline.readCSV(filePath);

        // Processar as vendas e gerar relatórios
        salesPipeline.processSales(salesData, mongoConnector);

        // Fechar a conexão com o MongoDB
        mongoConnector.close();
    }
}
