package sample.stream;

import akka.actor.ActorSystem;
import akka.dispatch.OnComplete;
import akka.stream.ActorFlowMaterializer;
import akka.stream.io.SynchronousFileSink;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GroupLogFile {
  public static void main(String[] args) throws IOException {
    final ActorSystem system = ActorSystem.create("Sys");
    final ActorFlowMaterializer materializer = ActorFlowMaterializer.create(system);

    final Pattern loglevelPattern = Pattern.compile(".*\\[(DEBUG|INFO|WARN|ERROR)\\].*");

    // read lines from a log file
    final String inPath = "src/main/resources/logfile.txt";
    final BufferedReader fileReader = new BufferedReader(new FileReader(inPath));

    Source.from(new FileIterable(fileReader)).
        // group them by log level
        groupBy(line -> {
          final Matcher matcher = loglevelPattern.matcher(line);
          if (matcher.find())
            return matcher.group(1);
          else
            return "OTHER";
        }).
        // write lines of each group to a separate file
        runForeach(levelProducerPair -> {
          final String outPath = "target/log-" + levelProducerPair.first() + ".txt";
          final PrintWriter output = new PrintWriter(new FileOutputStream(outPath), true);

          levelProducerPair.second().runForeach(output::println, materializer).
          // close resource when the group stream is completed
              onComplete(new OnComplete<BoxedUnit>() {
                @Override
                public void onComplete(Throwable failure, BoxedUnit success) throws Exception {
                  output.close();
                }
              }, system.dispatcher());
        }, materializer).onComplete(new OnComplete<BoxedUnit>() {
          @Override
          public void onComplete(Throwable failure, BoxedUnit success) throws Exception {
            try {
              fileReader.close();
            } catch (IOException ignore) {
            } finally {
              system.shutdown();
            }
          }
        }, system.dispatcher());
  }
}

class FileIterable implements Iterable<String> {

  private final BufferedReader fileReader;

  FileIterable(BufferedReader fileReader) {
    this.fileReader = fileReader;
  }

  @Override
  public Iterator<String> iterator() {
    return new Iterator<String>() {
      private String nextRow = null;
      private boolean active = false;

      @Override
      public boolean hasNext() {
        if (!active)
          nextRow = readNext();
        return nextRow != null;
      }

      @Override
      public String next() {
        String row = null;
        if (active) {
          row = nextRow;
        } else {
          row = readNext();
        }

        if (row == null) {
          throw new NoSuchElementException("No more rows");
        } else {
          nextRow = readNext();
          return row;
        }
      }

      private String readNext() {
        active = true;
        try {
          return fileReader.readLine();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}