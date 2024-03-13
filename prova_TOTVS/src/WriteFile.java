import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class WriteFile {
    public static void writeResultsToFile(String filename, List<String> itemsStartingWith0, List<String> itemsStartingWith1, long totalWordCount, long uniqueWordCount, JavaRDD<String> logs, JavaSparkContext sc) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            // Escrever itens que começam com 0
            writer.write("Itens que começam com 0: " + itemsStartingWith0.toString());
            writer.newLine();
            // Escrever itens que começam com 1
            writer.write("Itens que começam com 1: " + itemsStartingWith1.toString());
            writer.newLine();
            // Contar linhas começando com 0 e 1
            writer.write("Linhas que começam com 0: " + itemsStartingWith0.size());
            writer.newLine();
            writer.write("Linhas que começam com 1: " + itemsStartingWith1.size());
            writer.newLine();
            // Escrever resultados das análises
            writer.write("\nAnálises por linha começando com 1:");
            writer.newLine();
            // Contagem de palavras e ocorrências na segunda coluna onde a primeira é "1"
            for (String line : logs.filter(l -> l.startsWith("1")).collect()) {
                Tuple2<Long, Long> counts = LogAnalyzer.analyzeLogs(sc.parallelize(Collections.singletonList(line)));
                writer.write("Linha[" + line + "]: " + counts._1() + " palavras na segunda coluna e " + counts._2() + " palavras únicas");
                writer.newLine();
                // Contagem de ocorrências de cada palavra
                Map<String, Long> wordCountMap = logs.filter(l -> l.equals(line))
                        .flatMap(l -> Arrays.asList(l.split("\t")[1].split(" ")).iterator())
                        .countByValue();
                writer.write("Palavras e suas ocorrências:");
                writer.newLine();
                for (Map.Entry<String, Long> wordEntry : wordCountMap.entrySet()) {
                    writer.write(wordEntry.getKey() + ": " + wordEntry.getValue() + " vezes");
                    writer.newLine();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
