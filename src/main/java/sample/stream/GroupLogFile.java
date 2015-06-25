package sample.stream;

import akka.actor.ActorSystem;
import akka.dispatch.OnComplete;
import akka.stream.ActorMaterializer;
import akka.stream.io.Framing;
import akka.stream.io.SynchronousFileSource;
import akka.stream.stage.Context;
import akka.stream.stage.StageState;
import akka.stream.stage.StatefulStage;
import akka.stream.stage.SyncDirective;
import akka.util.ByteString;
import scala.runtime.BoxedUnit;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GroupLogFile {
  public static void main(String[] args) throws IOException {
    final ActorSystem system = ActorSystem.create("Sys");
    final ActorMaterializer materializer = ActorMaterializer.create(system);

    final Pattern loglevelPattern = Pattern.compile(".*\\[(DEBUG|INFO|WARN|ERROR)\\].*");

    // read lines from a log file
    final String inPath = "src/main/resources/logfile.txt";
    final File inputFile = new File(inPath);

    SynchronousFileSource.create(inputFile).
        // parse bytestrings (chunks of data) to lines
        via(Framing.delimiter(ByteString.fromString(System.lineSeparator()), 512, true)).
        map(bytestring -> bytestring.utf8String()).
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
            system.shutdown();
          }
        }, system.dispatcher());
  }

}
