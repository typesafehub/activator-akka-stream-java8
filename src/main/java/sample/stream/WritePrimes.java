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
import akka.japi.function.Procedure2;
import akka.stream.ActorFlowMaterializer;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.*;

public class WritePrimes {
  public static void main(String[] args) throws IOException {
    final ActorSystem system = ActorSystem.create("Sys");
    final ActorFlowMaterializer materializer = ActorFlowMaterializer.create(system);

    // generate random numbers
    final int maxRandomNumberSize = 1000000;
    final Source<Integer, BoxedUnit> primeSource = Source.from(new RandomIterable(maxRandomNumberSize)).
    // filter prime numbers
        filter(WritePrimes::isPrime).
        // and neighbor +2 is also prime
        filter(prime -> isPrime(prime + 2));

    // write to file sink
    final PrintWriter output = new PrintWriter(new FileOutputStream("target/primes.txt"), true);
    Sink<Integer, Future<BoxedUnit>> slowSink = Sink.foreach(prime -> {
      output.println(prime);
      // simulate slow consumer
        Thread.sleep(1000);
      });

    // console output sink
    Sink<Integer, Future<BoxedUnit>> consoleSink = Sink.foreach(System.out::println);

    // connect the graph, materialize and retrieve the completion Future
    final Future<BoxedUnit> future = FlowGraph.factory().closed(slowSink, (b, sink) -> {
      final UniformFanOutShape<Integer, Integer> bcast = b.graph(Broadcast.<Integer> create(2));
      b.from(primeSource).via(bcast).to(sink)
                        .from(bcast).to(consoleSink);
    }).run(materializer);
    
    future.onComplete(new OnComplete<BoxedUnit>() {
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