package sample.stream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import akka.actor.ActorSystem;
import akka.stream.FlowMaterializer;
import akka.stream.MaterializerSettings;
import akka.stream.Stop;
import akka.stream.javadsl.Flow;

public class GroupLogFile {
  public static void main(String[] args) throws IOException {
    final ActorSystem system = ActorSystem.create("Sys");
    final FlowMaterializer materializer = 
      FlowMaterializer.create(MaterializerSettings.create(), system);

    final Pattern loglevelPattern = Pattern.compile(".*\\[(DEBUG|INFO|WARN|ERROR)\\].*");

    // read lines from a log file
    final BufferedReader fileReader = new BufferedReader(
        new FileReader("src/main/resources/logfile.txt"));
    Flow.create(() -> {
        String line = fileReader.readLine();
        if (line == null) throw Stop.getInstance();
        else return line;
      }).
      // group them by log level
      groupBy(line -> {
        Matcher matcher = loglevelPattern.matcher(line);
        if (matcher.find()) return matcher.group(1);
        else return "OTHER";
      }).
      // write lines of each group to a separate file
      foreach(levelProducerPair -> {
          PrintWriter output = new PrintWriter(new FileOutputStream(
            "target/log-" + levelProducerPair.a() + ".txt"), true);
          Flow.create(levelProducerPair.b()).
            foreach(line -> output.println(line)).
            // close resource when the group stream is completed
            onComplete(materializer, e -> output.close());
      }).
      onComplete(materializer, e -> {
        if (e != null)
          System.out.println("Failure: " + e.getMessage());
        try { fileReader.close(); } catch (IOException ignore) {}
        system.shutdown();
      });
  }
}