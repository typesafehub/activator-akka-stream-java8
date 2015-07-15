package sample.stream;

import akka.actor.ActorSystem;
import akka.dispatch.OnComplete;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Tcp;
import akka.stream.javadsl.Tcp.IncomingConnection;
import akka.stream.javadsl.Tcp.ServerBinding;
import akka.util.ByteString;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

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

    final Sink<IncomingConnection, Future<BoxedUnit>> handler = Sink.foreach(conn -> {
      System.out.println("Client connected from: " + conn.remoteAddress());
      conn.handleWith(Flow.<ByteString>empty(), materializer);
    });

    final Future<ServerBinding> bindingFuture =
      Tcp.get(system).bind(serverAddress.getHostString(), serverAddress.getPort()).to(handler).run(materializer);

    bindingFuture.onSuccess(new OnSuccess<ServerBinding>() {
      @Override
      public void onSuccess(ServerBinding binding) {
        System.out.println("Server started, listening on: " + binding.localAddress());
      }
    }, system.dispatcher());

    bindingFuture.onFailure(new OnFailure() {
      @Override
      public void onFailure(Throwable e) {
        System.err.println("Server could not bind to " + serverAddress + " : " + e.getMessage());
        system.shutdown();
      }
    }, system.dispatcher());

  }

  public static void client(ActorSystem system, InetSocketAddress serverAddress) {
    final ActorMaterializer materializer = ActorMaterializer.create(system);

    final List<ByteString> testInput = new ArrayList<>();
    for (char c = 'a'; c <= 'z'; c++) {
      testInput.add(ByteString.fromString(String.valueOf(c)));
    }

    Source<ByteString, BoxedUnit> responseStream =
      Source.from(testInput).via(Tcp.get(system).outgoingConnection(serverAddress.getHostString(), serverAddress.getPort()));

    Future<ByteString> result = responseStream.runFold(
      ByteString.empty(), (acc, in) -> acc.concat(in), materializer);

    result.onComplete(new OnComplete<ByteString>() {
      @Override
      public void onComplete(Throwable failure, ByteString success) throws Exception {
        if (failure != null) {
          System.err.println("Failure: " + failure.getMessage());
        } else {
          System.out.println("Result: " + success.utf8String());
        }
        System.out.println("Shutting down client");
        system.shutdown();
      }
    }, system.dispatcher());

  }

}
