import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import java.util.*;
import java.util.regex.Pattern;

public class LogAnalyzer {
    private static final Pattern TAB_PATTERN = Pattern.compile("\t");

    public static Tuple2<List<String>, List<String>> processLogs(JavaRDD<String> logs) {
        // Divisão do log em duas partes
        JavaRDD<String> linesStartingWith0 = filterLinesStartingWith(logs, "0");
        JavaRDD<String> linesStartingWith1 = filterLinesStartingWith(logs, "1");

        // Obter itens que começam com 0 e 1
        List<String> itemsStartingWith0 = linesStartingWith0
                .map(line -> TAB_PATTERN.split(line)[1])
                .collect();
        List<String> itemsStartingWith1 = linesStartingWith1
                .map(line -> TAB_PATTERN.split(line)[1])
                .collect();

        return new Tuple2<>(itemsStartingWith0, itemsStartingWith1);
    }

    private static JavaRDD<String> filterLinesStartingWith(JavaRDD<String> lines, String prefix) {
        return lines.filter(line -> line.startsWith(prefix));
    }

    public static Tuple2<Long, Long> analyzeLogs(JavaRDD<String> logs) {
        // Contagem de linhas começando com 0
        long countLinesStartingWith0 = countLines(filterLinesStartingWith(logs, "0"));
        System.out.println("Linhas que começam com 0: " + countLinesStartingWith0);

        // Contagem de palavras e palavras únicas começando com 1
        return countWords(filterLinesStartingWith(logs, "1"));
    }

    private static long countLines(JavaRDD<String> lines) {
        return lines.count();
    }

    private static Tuple2<Long, Long> countWords(JavaRDD<String> lines) {
        // Contagem de palavras
        JavaRDD<String[]> wordsRDD = lines.map(line -> TAB_PATTERN.split(line));
        JavaRDD<String> wordsStartingWith1 = wordsRDD.filter(words -> words[0].equals("1")).flatMap(words -> Arrays.asList(words[1].split(" ")).iterator());

        // Contagem de palavras únicas
        JavaRDD<String> uniqueWordsStartingWith1 = wordsStartingWith1.distinct();

        // Contagem de palavras totais e únicas
        long totalWordCount = wordsStartingWith1.count();
        long uniqueWordCount = uniqueWordsStartingWith1.count();

        // Contagem de palavras por linha e palavras únicas por linha
        Map<String, Tuple2<Long, Long>> wordCountsByLine = wordsRDD.filter(words -> words[0].equals("1"))
                .mapToPair(words -> {
                    String line = String.join("\t", words);
                    String[] wordArray = words[1].split(" ");
                    long wordCount = wordArray.length;
                    long uniqueWordCountPerLine = Arrays.stream(wordArray).distinct().count();
                    return new Tuple2<>(line, new Tuple2<>(wordCount, uniqueWordCountPerLine));
                })
                .collectAsMap();

        // Imprimir contagem de palavras por linha e palavras únicas por linha
        for (Map.Entry<String, Tuple2<Long, Long>> entry : wordCountsByLine.entrySet()) {
            long wordCount = entry.getValue()._1();
            long uniqueWordCountPerLine = entry.getValue()._2();
            System.out.println("Linha[" + entry.getKey() + "]: " + wordCount + " palavras na segunda coluna e " + uniqueWordCountPerLine + " palavras únicas");
        }

        return new Tuple2<>(totalWordCount, uniqueWordCount);
    }
}


