package sample.stream;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.dispatch.OnComplete;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import akka.japi.pf.PFBuilder;
import akka.stream.ActorMaterializer;
import akka.stream.TLSClientAuth;
import akka.stream.TLSProtocol;
import akka.stream.TLSRole;
import akka.stream.javadsl.*;
import akka.stream.javadsl.Tcp.IncomingConnection;
import akka.stream.javadsl.Tcp.ServerBinding;
import akka.util.ByteString;
import com.typesafe.sslconfig.akka.AkkaSSLConfig;
import com.typesafe.sslconfig.akka.util.AkkaLoggerFactory;
import com.typesafe.sslconfig.ssl.*;
import com.typesafe.sslconfig.util.LoggerFactory;
import scala.PartialFunction;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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

    final Sink<IncomingConnection, CompletionStage<Done>> handler = Sink.foreach(conn -> {
      System.out.println("Client connected from: " + conn.remoteAddress());
      conn.handleWith(Flow.<ByteString>create().log("Server incoming bytes").via(tlsStage(system, TLSRole.server()).join(Flow.<ByteString>create().log("in the server handler"))), materializer);
    });


    final CompletionStage<ServerBinding> bindingFuture =
      Tcp.get(system).bind(serverAddress.getHostString(), serverAddress.getPort()).to(handler).run(materializer);

    bindingFuture.handle((ServerBinding binding, Throwable exception) -> {
      if (binding != null) {
        System.out.println("Server started, listening on: " + binding.localAddress());
      } else {
        System.err.println("Server could not bind to " + serverAddress + " : " + exception.getMessage());
        //system.shutdown();
      }
      return NotUsed.getInstance();
    });

  }

  private static BidiFlow<ByteString, ByteString, ByteString, ByteString, NotUsed> tlsStage(ActorSystem system, TLSRole role) {
    final AkkaSSLConfig sslConfig = AkkaSSLConfig.get(system);
    final SSLConfigSettings config = sslConfig.config();
    final LoggerFactory logger = new AkkaLoggerFactory(system);

    // initial ssl context
    final KeyManagerFactoryWrapper keyManagerFactory = sslConfig.buildKeyManagerFactory(config);
    final TrustManagerFactoryWrapper trustManagerFactory = sslConfig.buildTrustManagerFactory(config);
    final SSLContext sslContext = new ConfigSSLContextBuilder(logger, config, keyManagerFactory, trustManagerFactory).build();

    // protocols
    final SSLParameters defaultParams = sslContext.getDefaultSSLParameters();
    final String[] defaultProtocols = defaultParams.getProtocols();
    final String[] protocols = sslConfig.configureProtocols(defaultProtocols, config);
    defaultParams.setProtocols(protocols);

    // ciphers
    final String[] defaultCiphers = defaultParams.getCipherSuites();
    final String[] cipherSuites = sslConfig.configureCipherSuites(defaultCiphers, config);
    defaultParams.setCipherSuites(cipherSuites);

    // auth
    final Optional<TLSClientAuth> clientAuth = getClientAuth(config.sslParametersConfig().clientAuth());

    // hostname!
    if (!config.loose().disableHostnameVerification()) {
      defaultParams.setEndpointIdentificationAlgorithm("https");
    }

    TLSProtocol.NegotiateNewSession firstSession = TLSProtocol.NegotiateNewSession$.MODULE$
      .withDefaults()
      .withCipherSuites(cipherSuites)
      .withProtocols(protocols)
      .withParameters(defaultParams);

    if (clientAuth.isPresent()) {
      firstSession = firstSession.withClientAuth(clientAuth.get());
    }

    final BidiFlow<TLSProtocol.SslTlsOutbound, ByteString, ByteString, TLSProtocol.SslTlsInbound, NotUsed> tls =
      TLS.create(sslContext, firstSession, role);

    final PartialFunction<TLSProtocol.SslTlsInbound, ByteString> pf =
      new PFBuilder()
        .match(TLSProtocol.SessionBytes.class, (sb) -> ((TLSProtocol.SessionBytes)sb).bytes())
        .match(TLSProtocol.SslTlsInbound.class, (ib) -> {
          System.out.println("Received other that SeesionBytes" + ib);
          return null;
        })
        .build();

    final BidiFlow<ByteString, TLSProtocol.SslTlsOutbound, TLSProtocol.SslTlsInbound, ByteString, NotUsed> tlsSupport =
      BidiFlow.fromFlows(
        Flow.<ByteString>create().map(TLSProtocol.SendBytes::new),
        Flow.<TLSProtocol.SslTlsInbound>create().collect(pf));

    return tlsSupport.atop(tls);
  }

  private static Optional<TLSClientAuth> getClientAuth(ClientAuth auth) {
    if (auth.equals(ClientAuth.want())) { return Optional.of(TLSClientAuth.want()); }
    else if (auth.equals(ClientAuth.need())) { return Optional.of(TLSClientAuth.need()); }
    else if (auth.equals(ClientAuth.none())) { return Optional.of(TLSClientAuth.none()); }
    else { return Optional.empty(); }
  }

  public static void client(ActorSystem system, InetSocketAddress serverAddress) {
    final ActorMaterializer materializer = ActorMaterializer.create(system);

    final List<ByteString> testInput = new ArrayList<>();
    for (char c = 'a'; c <= 'z'; c++) {
      testInput.add(ByteString.fromString(String.valueOf(c)));
    }

    final Sink<ByteString, CompletionStage<ByteString>> sink = Sink.fold(
      ByteString.empty(), (acc, in) -> acc.concat(in));

    final Source<ByteString, NotUsed> source = Source.from(testInput).log("Element out");

    final CompletionStage<ByteString> result = Flow.fromSinkAndSourceMat(sink, source, Keep.left())
      .joinMat(tlsStage(system, TLSRole.client()), Keep.left())
      .joinMat(Flow.<ByteString>create().log("Before TCP").via(Tcp.get(system).outgoingConnection(serverAddress.getHostString(), serverAddress.getPort())), Keep.left())
      .run(materializer);

    result.handle((success, failure) -> {
      if (failure != null) {
        System.err.println("Failure: " + failure.getMessage());
      } else {
        System.out.println("Result: " + success.utf8String());
      }
      System.out.println("Shutting down client");
      //system.shutdown();
      return NotUsed.getInstance();
    });
  }

}
