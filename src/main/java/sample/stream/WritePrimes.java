package sample.stream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import scala.concurrent.Future;
import scala.concurrent.forkjoin.ThreadLocalRandom;
import scala.runtime.BoxedUnit;
import akka.actor.ActorSystem;
import akka.dispatch.OnComplete;
import akka.stream.FlowMaterializer;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.FlowGraph;
import akka.stream.javadsl.KeyedSink;
import akka.stream.javadsl.MaterializedMap;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

public class WritePrimes {
  public static void main(String[] args) throws IOException {
    final ActorSystem system = ActorSystem.create("Sys");
    final FlowMaterializer materializer = FlowMaterializer.create(system);

    // generate random numbers
    final int maxRandomNumberSize = 1000000;
    final Source<Integer> primeSource = Source.from(new RandomIterable(maxRandomNumberSize)).
    // filter prime numbers
        filter(WritePrimes::isPrime).
        // and neighbor +2 is also prime
        filter(prime -> isPrime(prime + 2));

    // write to file sink
    final PrintWriter output = new PrintWriter(new FileOutputStream("target/primes.txt"), true);
    KeyedSink<Integer, Future<BoxedUnit>> slowSink = Sink.foreach(prime -> {
      output.println(prime);
      // simulate slow consumer
        Thread.sleep(1000);
      });

    // console output sink
    KeyedSink<Integer, Future<BoxedUnit>> consoleSink = Sink.foreach(System.out::println);

    // connect the graph
    Broadcast<Integer> broadcast = Broadcast.create();
    FlowGraph graph = FlowGraph.builder().addEdge(primeSource, broadcast).addEdge(broadcast, slowSink)
        .addEdge(broadcast, consoleSink).build();

    // and then run it (or call `run` directly on the builder)
    MaterializedMap materialized = graph.run(materializer);

    materialized.get(slowSink).onComplete(new OnComplete<BoxedUnit>() {
      @Override
      public void onComplete(Throwable failure, BoxedUnit success) throws Exception {
        if (failure != null) {
          System.err.println("Failure: " + failure);
        }
        try {
          output.close();
        } catch (Exception ignore) {
        } finally {
          system.shutdown();
        }
      }
    }, system.dispatcher());
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