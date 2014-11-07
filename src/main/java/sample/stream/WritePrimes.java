package sample.stream;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.IOException;
import scala.concurrent.forkjoin.ThreadLocalRandom;
import org.reactivestreams.api.Producer;
import akka.actor.ActorSystem;
import akka.stream.FlowMaterializer;
import akka.stream.MaterializerSettings;
import akka.stream.javadsl.Flow;

public class WritePrimes {
  public static void main(String[] args) throws IOException {
    final ActorSystem system = ActorSystem.create("Sys");
    final FlowMaterializer materializer = 
      FlowMaterializer.create(MaterializerSettings.create(), system);

    // generate random numbers
    final int maxRandomNumberSize = 1000000;
    final Producer<Integer> producer =
      Flow.create(() -> ThreadLocalRandom.current().nextInt(maxRandomNumberSize)).
        // filter prime numbers
        filter(rnd -> isPrime(rnd)).
        // and neighbor +2 is also prime
        filter(prime -> isPrime(prime + 2)).
        toProducer(materializer);

    // connect two consumer flows to the producer  

    // write to file  
    final PrintWriter output = new PrintWriter(new FileOutputStream("target/primes.txt"), true);
    Flow.create(producer).
      foreach(prime -> {
        output.println(prime);
        // simulate slow consumer
        Thread.sleep(1000);
      }).
      onComplete(materializer, e -> {
        output.close();
        system.shutdown();
      });

    // write to console  
    Flow.create(producer).
      foreach(prime -> System.out.println(prime)).
      consume(materializer);
      
  }

  private static boolean isPrime(int n) {
    if (n <= 1) return false;
    else if (n == 2) return true;
    else {
      for (int i = 2; i < n; i++) {
        if (n % i == 0) return false;
      }
      return true;
    } 
  }
}
