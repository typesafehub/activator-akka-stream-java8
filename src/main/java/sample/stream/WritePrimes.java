package sample.stream;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import scala.concurrent.forkjoin.ThreadLocalRandom;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

public class WritePrimes {
  public static void main(String[] args) throws IOException {
    final ActorSystem system = ActorSystem.create("Sys");
    final ActorMaterializer materializer = ActorMaterializer.create(system);

    // generate random numbers
    final int maxRandomNumberSize = 1000000;
    final Source<Integer, NotUsed> primeSource = Source.from(new RandomIterable(maxRandomNumberSize)).
         // filter prime numbers
        filter(WritePrimes::isPrime).
        // and neighbor +2 is also prime
        filter(prime -> isPrime(prime + 2));

    // write to file sink
    Sink<ByteString, CompletionStage<IOResult>> output = FileIO.toFile(new File("target/primes.txt"));
    Sink<Integer, CompletionStage<IOResult>> slowSink =
      Flow.of(Integer.class)
      .map(i -> {
        // simulate slow consumer
        Thread.sleep(1000);
        return ByteString.fromString(i.toString());
      }).toMat(output, Keep.right());

    // console output sink
    Sink<Integer, CompletionStage<Done>> consoleSink = Sink.<Integer>foreach(System.out::println);

    // connect the graph, materialize and retrieve the completion Future
    final Graph<ClosedShape, CompletionStage<IOResult>> graph = GraphDSL.create(slowSink, (b, sink) -> {
      final UniformFanOutShape<Integer, Integer> bcast = b.add(Broadcast.<Integer> create(2));
      b.from(b.add(primeSource)).viaFanOut(bcast).to(sink)
                                     .from(bcast).to(b.add(consoleSink));
      return ClosedShape.getInstance();
    });
    final CompletionStage<IOResult> future = RunnableGraph.fromGraph(graph).run(materializer);

    future.handle((ioResult, failure) -> {
      if (failure != null) System.err.println("Failure: " + failure);
      else if (!ioResult.wasSuccessful()) System.err.println("Writing to file failed " + ioResult.getError());
      else System.out.println("Successfully wrote " + ioResult.getCount() + " bytes");
      system.terminate();
      return NotUsed.getInstance();
    });
  }

  private static boolean isPrime(int n) {
    if (n <= 1)
      return false;
    else if (n == 2)
      return true;
    else {
      for (int i = 2; i < n; i++) {
        if (n % i == 0)
          return false;
      }
      return true;
    }
  }
}

class RandomIterable implements Iterable<Integer> {

  private final int maxRandomNumberSize;

  RandomIterable(int maxRandomNumberSize) {
    this.maxRandomNumberSize = maxRandomNumberSize;
  }

  @Override
  public Iterator<Integer> iterator() {
    return new Iterator<Integer>() {
      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public Integer next() {
        return ThreadLocalRandom.current().nextInt(maxRandomNumberSize);
      }
    };
  }
}
