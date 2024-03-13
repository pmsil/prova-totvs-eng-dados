import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.List;


public class Principal {
    public static void main(String[] args) {
        // Configuração do Spark
        SparkConf conf = new SparkConf().setAppName("LogProcessor").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Carregamento do arquivo de log
        JavaRDD<String> logs = sc.textFile("logs.txt");

        // Processar logs e obter resultados
        Tuple2<List<String>, List<String>> logItems = LogAnalyzer.processLogs(logs);
        Tuple2<Long, Long> analysisResults = LogAnalyzer.analyzeLogs(logs);

        // Gravar resultados em arquivo de texto
        WriteFile.writeResultsToFile("analise_logs.txt", logItems._1(), logItems._2(), analysisResults._1(), analysisResults._2(), logs, sc);

        // Encerramento do contexto do Spark
        sc.stop();
    }
}