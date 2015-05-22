package sample.stream;

import akka.actor.ActorSystem;
import akka.dispatch.OnComplete;
import akka.stream.ActorFlowMaterializer;
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
    final ActorFlowMaterializer materializer = ActorFlowMaterializer.create(system);

    final Pattern loglevelPattern = Pattern.compile(".*\\[(DEBUG|INFO|WARN|ERROR)\\].*");

    // read lines from a log file
    final String inPath = "src/main/resources/logfile.txt";
    final File inputFile = new File(inPath);

    SynchronousFileSource.create(inputFile).
        // parse bytestrings (chunks of data) to lines
        transform(() -> parseLines("\n", 512)).
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

  public static StatefulStage<ByteString, String> parseLines(String separator, int maximumLineBytes) {
    return new StatefulStage<ByteString, String>() {

      final ByteString separatorBytes = ByteString.fromString(separator);
      final byte firstSeparatorByte = separatorBytes.head();

      @Override
      public StageState<ByteString, String> initial() {
        return new StageState<ByteString, String>() {
          ByteString buffer = ByteString.empty();
          int nextPossibleMatch = 0;

          @Override
          public SyncDirective onPush(ByteString chunk, Context<String> ctx) {
            buffer = buffer.concat(chunk);
            if (buffer.size() > maximumLineBytes) {
              return ctx.fail(new IllegalStateException("Read " + buffer.size() + " bytes " +
                                                          "which is more than " + maximumLineBytes + " without seeing a line terminator"));
            } else {
              return emit(doParse().iterator(), ctx);
            }
          }

          private List<String> doParse() {
            List<String> parsedLinesSoFar = new ArrayList<String>();
            while (true) {
              int possibleMatchPos = buffer.indexOf(firstSeparatorByte, nextPossibleMatch);
              if (possibleMatchPos == -1) {
                // No matching character, we need to accumulate more bytes into the buffer
                nextPossibleMatch = buffer.size();
                break;
              } else if (possibleMatchPos + separatorBytes.size() > buffer.size()) {
                // We have found a possible match (we found the first character of the terminator
                // sequence) but we don't have yet enough bytes. We remember the position to
                // retry from next time.
                nextPossibleMatch = possibleMatchPos;
                break;
              } else {
                if (buffer.slice(possibleMatchPos, possibleMatchPos + separatorBytes.size())
                          .equals(separatorBytes)) {
                  // Found a match
                  String parsedLine = buffer.slice(0, possibleMatchPos).utf8String();
                  buffer = buffer.drop(possibleMatchPos + separatorBytes.size());
                  nextPossibleMatch -= possibleMatchPos + separatorBytes.size();
                  parsedLinesSoFar.add(parsedLine);
                } else {
                  nextPossibleMatch += 1;
                }
              }
            }
            return parsedLinesSoFar;
          }

        };
      }

    };
  }

}

