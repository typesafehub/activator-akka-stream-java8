package sample.stream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import akka.stream.io.SynchronousFileSink;
import akka.stream.io.SynchronousFileSource;
import akka.util.ByteString;
import scala.Function1;
import scala.concurrent.Future;
import scala.concurrent.forkjoin.ThreadLocalRandom;
import scala.runtime.BoxedUnit;
import akka.actor.ActorSystem;
import akka.dispatch.OnComplete;
import scala.concurrent.Future;
import akka.japi.function.Procedure2;
import akka.stream.ActorMaterializer;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.*;
import scala.util.Try;

public class WritePrimes {
  public static void main(String[] args) throws IOException {
    final ActorSystem system = ActorSystem.create("Sys");
    final ActorMaterializer materializer = ActorMaterializer.create(system);

    // generate random numbers
    final int maxRandomNumberSize = 1000000;
    final Source<Integer, BoxedUnit> primeSource = Source.from(new RandomIterable(maxRandomNumberSize)).
    // filter prime numbers
        filter(WritePrimes::isPrime).
        // and neighbor +2 is also prime
        filter(prime -> isPrime(prime + 2));

    // write to file sink
    Sink<ByteString, Future<Long>> output = SynchronousFileSink.create(new File("target/primes.txt"));
    Sink<Integer, Future<Long>> slowSink =
      Flow.of(Integer.class)
      .map(i -> {
        // simulate slow consumer
        Thread.sleep(1000);
        return ByteString.fromString(i.toString());
      }).<Future<Long>, Future<Long>>toMat(output, (unit, flong) -> flong);

    // console output sink
    Sink<Integer, Future<BoxedUnit>> consoleSink = Sink.foreach(System.out::println);

    // connect the graph, materialize and retrieve the completion Future
    final Future<Long> future = FlowGraph.factory().closed(slowSink, (b, sink) -> {
      final UniformFanOutShape<Integer, Integer> bcast = b.graph(Broadcast.<Integer> create(2));
      b.from(primeSource).via(bcast).to(sink)
                        .from(bcast).to(consoleSink);
    }).run(materializer);

    future.onComplete(new OnComplete<Long>() {
      @Override
      public void onComplete(Throwable failure, Long success) throws Exception {
        if (failure != null) System.err.println("Failure: " + failure);
        system.shutdown();
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
