package sample.stream;

import java.util.Arrays;
import akka.actor.ActorSystem;
import akka.stream.FlowMaterializer;
import akka.stream.MaterializerSettings;
import akka.stream.javadsl.Flow;

public class BasicTransformation {
  public static void main(String[] args) {
    final ActorSystem system = ActorSystem.create("Sys");
    final FlowMaterializer materializer = 
      FlowMaterializer.create(MaterializerSettings.create(), system);

    final String text =
      "Lorem Ipsum is simply dummy text of the printing and typesetting industry. " +
      "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, " +
      "when an unknown printer took a galley of type and scrambled it to make a type " +
      "specimen book.";

    Flow.create(Arrays.asList(text.split("\\s"))).
      // transform
      map(line -> line.toUpperCase()).
      // print to console
      foreach(tranformedLine -> System.out.println(tranformedLine)).
      onComplete(materializer, e -> {
        if (e != null)
          System.out.println("Failure: " + e.getMessage());
        system.shutdown();
      });
  }
}