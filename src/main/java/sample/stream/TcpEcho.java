package sample.stream;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.dispatch.OnComplete;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import akka.stream.ActorMaterializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.*;
import akka.stream.javadsl.Tcp.IncomingConnection;
import akka.stream.javadsl.Tcp.ServerBinding;
import akka.util.ByteString;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class TcpEcho {

  /**
   * Use without parameters to start both client and server.
   *
   * Use parameters `server 0.0.0.0 6001` to start server listening on port
   * 6001.
   *
   * Use parameters `client 127.0.0.1 6001` to start client connecting to server
   * on 127.0.0.1:6001.
   *
   */
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      ActorSystem system = ActorSystem.create("ClientAndServer");
      InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 6000);
      server(system, serverAddress);
      client(system, serverAddress);
    } else {
      InetSocketAddress serverAddress;
      if (args.length == 3) {
        serverAddress = new InetSocketAddress(args[1], Integer.valueOf(args[2]));
      } else {
        serverAddress = new InetSocketAddress("127.0.0.1", 6000);
      }
      if (args[0].equals("server")) {
        ActorSystem system = ActorSystem.create("Server");
        server(system, serverAddress);
      } else if (args[0].equals("client")) {
        ActorSystem system = ActorSystem.create("Client");
        client(system, serverAddress);
      }
    }
  }

  public static void server(ActorSystem system, InetSocketAddress serverAddress) {
    final ActorMaterializer materializer = ActorMaterializer.create(system);

    final List<ByteString> testInput = new ArrayList<>();
    for (char c = 'a'; c <= 'z'; c++) {
      // Note all commands are \n-terminated, even the last one.
      testInput.add(ByteString.fromString("COMMAND " + String.valueOf(c) + "\n"));
    }

    final Sink<ByteString, CompletionStage<Done>> responseFromClientSink =
      Framing.delimiter(ByteString.fromString("\n"), 120)
        .toMat(Sink.foreach(bs -> System.out.println("Got response from client: " + bs.utf8String())), Keep.right());

    final Sink<IncomingConnection, CompletionStage<Done>> handler = Sink.foreach(conn -> {
      System.out.println("Client connected from: " + conn.remoteAddress());
      Flow<ByteString, ByteString, NotUsed> flow = Flow.fromSinkAndSource(responseFromClientSink, Source.from(testInput));
      conn.handleWith(flow, materializer);
    });

    final CompletionStage<ServerBinding> bindingFuture =
      Tcp.get(system).bind(serverAddress.getHostString(), serverAddress.getPort()).to(handler).run(materializer);

    bindingFuture.handle((ServerBinding binding, Throwable exception) -> {
      if (binding != null) {
        System.out.println("Server started, listening on: " + binding.localAddress());
      } else {
        System.err.println("Server could not bind to " + serverAddress + " : " + exception.getMessage());
        system.terminate();
      }
      return NotUsed.getInstance();
    });
  }

  public static void client(ActorSystem system, InetSocketAddress serverAddress) {
    final ActorMaterializer materializer = ActorMaterializer.create(system);

    Flow<ByteString, ByteString, NotUsed> commandHandling = Flow.fromFunction(bs -> {
      System.out.println("Handling input " + bs.utf8String());
      return ByteString.fromString("response to " + bs.utf8String() + "\n");
    });

    Flow<ByteString, ByteString, NotUsed> streamHandling =
      Framing.delimiter(ByteString.fromString("\n"), 120)
        .via(commandHandling);

    Flow<ByteString, ByteString, CompletionStage<Tcp.OutgoingConnection>> connectionFlow =
      Tcp.get(system).outgoingConnection(serverAddress.getHostString(), serverAddress.getPort());

    CompletionStage<Tcp.OutgoingConnection> result =
      connectionFlow
        .join(streamHandling)
        .run(materializer);

    result.handle((success, failure) -> {
      if (failure != null) {
        system.log().info("Failure: " + failure.getMessage());
      } else {
        system.log().info("Connected, handling commands from server");
      }
      return NotUsed.getInstance();
    });
  }

}
