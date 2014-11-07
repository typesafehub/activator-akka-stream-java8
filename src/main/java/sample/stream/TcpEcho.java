package sample.stream;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;

import akka.dispatch.OnComplete;
import akka.japi.JavaPartialFunction;
import akka.japi.pf.FI;
import akka.japi.pf.ReceiveBuilder;
import akka.japi.pf.UnitPFBuilder;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.reactivestreams.Publisher;
import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.Future;
import akka.actor.ActorSystem;
import akka.dispatch.OnSuccess;
import akka.dispatch.OnFailure;
import static akka.pattern.Patterns.ask;
import akka.stream.FlowMaterializer;
import akka.stream.MaterializerSettings;
import akka.stream.io.StreamTcp;
import akka.stream.io.StreamTcpMessage;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;
import akka.util.Timeout;
import scala.util.Failure;
import scala.util.Success;

public class TcpEcho {

  /**
   * Use without parameters to start both client and
   * server.
   *
   * Use parameters `server 0.0.0.0 6001` to start server listening on port 6001.
   *
   * Use parameters `client 127.0.0.1 6001` to start client connecting to
   * server on 127.0.0.1:6001.
   *
   */
  public static void main(String[] args) {
    if (args.length == 0) {
      ActorSystem system = ActorSystem.create("ClientAndServer");
      InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 6000);
      server(system, serverAddress);
      client(system, serverAddress);
    } else {
      InetSocketAddress serverAddress;
      if (args.length == 3)
        serverAddress = new InetSocketAddress(args[1], Integer.valueOf(args[2]));
      else
        serverAddress = new InetSocketAddress("127.0.0.1", 6000);
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
    final FlowMaterializer materializer = FlowMaterializer.create(system);
    final Timeout timeout = new Timeout(FiniteDuration.create(5, TimeUnit.SECONDS));

    final Future<Object> serverFuture =
      ask(StreamTcp.get(system).manager(),
        StreamTcpMessage.bind(MaterializerSettings.create(system), serverAddress), timeout);

    serverFuture.onSuccess(new OnSuccess<Object>() {
      public void onSuccess(Object result) {
        if (result instanceof StreamTcp.TcpServerBinding) {
          StreamTcp.TcpServerBinding serverBinding = (StreamTcp.TcpServerBinding) result;
          System.out.println("Server started, listening on: " + serverBinding.localAddress());

          Source.from(serverBinding.connectionStream()).
            foreach(conn -> {
              System.out.println("Client connected from: " + conn.remoteAddress());
              conn.inputStream().subscribe(conn.outputStream());
            }, materializer);
        }
      }}, system.dispatcher());

    serverFuture.onFailure(new OnFailure() {
      public void onFailure(Throwable e) {
        System.err.println("Server could not bind to " + serverAddress + " : " + e.getMessage());
        system.shutdown();
      }
    }, system.dispatcher());

  }

  public static void client(ActorSystem system, InetSocketAddress serverAddress) {
    final MaterializerSettings settings = MaterializerSettings.create(system);
    final FlowMaterializer materializer = FlowMaterializer.create(settings, system);
    final Timeout timeout = new Timeout(FiniteDuration.create(5, TimeUnit.SECONDS));

    final List<ByteString> testInput = new ArrayList<>();
    for (char c = 'a'; c <= 'z'; c++) {
      testInput.add(ByteString.fromString(String.valueOf(c)));
    }

    Future<Object> clientFuture = ask(StreamTcp.get(system).manager(), StreamTcpMessage.connect(settings, serverAddress), timeout);

    clientFuture.onSuccess(new OnSuccess<Object>() {
      public void onSuccess(Object result) {
          if (result instanceof StreamTcp.OutgoingTcpConnection) {
            StreamTcp.OutgoingTcpConnection clientBinding = (StreamTcp.OutgoingTcpConnection) result;

            Publisher<ByteString> byteStringPublisher = Source.from(testInput).runWith(Sink.publisher(), materializer);

            byteStringPublisher.subscribe(clientBinding.outputStream());

            Future<List<Character>> foldFuture = Source.from(clientBinding.inputStream()).
              fold(new ArrayList<Character>(), (acc, in) -> {
                for (byte b : in.toArray()) {
                  acc.add((char) b);
                }
                return acc;
              }, materializer);

            foldFuture.
              onComplete(new OnComplete<List<Character>>() {
                @Override public void onComplete(Throwable failure, List<Character> success) throws Exception {
                  if (failure != null) {
                    System.err.println("Failure: " + failure.getMessage());
                  } else {
                    System.out.println("Result: " + success);
                  }
                  System.out.println("Shutting down client");
                  system.shutdown();
                }
              }, system.dispatcher());
          }
    }}, system.dispatcher());

    clientFuture.onFailure(new OnFailure() {
      public void onFailure(Throwable e) {
        System.err.println("Client could not connect to " + serverAddress + " : " + e.getMessage());
        system.shutdown();
      }
    }, system.dispatcher());

  }

}
