package sample.stream;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
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
import com.typesafe.sslconfig.ssl.ClientAuth;
import com.typesafe.sslconfig.ssl.KeyManagerFactoryWrapper;
import com.typesafe.sslconfig.ssl.SSLConfigSettings;
import scala.PartialFunction;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletionStage;

public class TcpTLSEcho {

  /**
   * Use without parameters to start both client and server.
   * <p>
   * Use parameters `server 0.0.0.0 6001` to start server listening on port
   * 6001.
   * <p>
   * Use parameters `client 127.0.0.1 6001` to start client connecting to server
   * on 127.0.0.1:6001.
   */
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      ActorSystem serverSystem = ActorSystem.create("Server");
      ActorSystem clientSystem = ActorSystem.create("Client");
      InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 6000);

      server(serverSystem, serverAddress);

      // http://typesafehub.github.io/ssl-config/CertificateGeneration.html#client-configuration
      client(clientSystem, serverAddress);

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
  
  // ------------------------ server ------------------------------ 
  
  public static void server(ActorSystem system, InetSocketAddress serverAddress) {
    final ActorMaterializer materializer = ActorMaterializer.create(system);

    Random random = new Random();

    Flow<ByteString, ByteString, NotUsed> handling = Flow.fromFunction((ByteString response) -> {
      system.log().info("Got response \"" + response.utf8String() + "\"");
      // Sending next command
      return ByteString.fromString("Subsequent command " + random.nextInt() + "\n");
    }).prepend(Source.single(ByteString.fromString("Initial command\n")));

    final Sink<IncomingConnection, CompletionStage<Done>> handler = Sink.foreach(conn -> {
      system.log().info("Client connected from: " + conn.remoteAddress());
      Flow<ByteString, ByteString, NotUsed> flow = Flow.<ByteString>create()
        // .log("Server raw incoming bytes") // uncomment to see encrypted raw bytes
        .via(tlsStage(system, TLSRole.server()).reversed()
          .join(handling));

        conn.handleWith(flow, materializer);
    });

    final CompletionStage<ServerBinding> bindingFuture =
      Tcp.get(system).bind(serverAddress.getHostString(), serverAddress.getPort()).to(handler).run(materializer);

    bindingFuture.handle((ServerBinding binding, Throwable exception) -> {
      if (binding != null) {
        system.log().info("Server started, listening on: " + binding.localAddress());
      } else {
        system.log().info("Server could not bind to " + serverAddress + " : " + exception.getMessage());
        //system.shutdown();
      }
      return NotUsed.getInstance();
    });

  }

  // ------------------------ end of server ------------------------------ 


  // ------------------------ client ------------------------------ 
  
  public static void client(ActorSystem system, InetSocketAddress serverAddress) throws Exception {
    final ActorMaterializer materializer = ActorMaterializer.create(system);

    Flow<ByteString, ByteString, NotUsed> commandHandling = Flow.fromFunction(bs -> {
      system.log().info("Handling input " + bs.utf8String());
      return ByteString.fromString("response to '" + bs.utf8String() + "'\n");
    });

    Flow<ByteString, ByteString, NotUsed> streamHandling =
      Framing.delimiter(ByteString.fromString("\n"), 120)
        .via(commandHandling);

    final Flow<ByteString, ByteString, CompletionStage<Tcp.OutgoingConnection>> connection =
      Tcp.get(system).outgoingConnection(serverAddress.getHostString(), serverAddress.getPort());

    final CompletionStage<Tcp.OutgoingConnection> result =
      connection
        .join(tlsStage(system, TLSRole.client()).reversed())
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

  // ------------------------ end of client ------------------------------ 

  
  // ------------------------ setting up TLS ------------------------
  
  @SuppressWarnings("unchecked")
  private static BidiFlow<ByteString, ByteString, ByteString, ByteString, NotUsed> tlsStage(ActorSystem system, TLSRole role) throws Exception {
    final AkkaSSLConfig sslConfig = AkkaSSLConfig.get(system);

    final SSLConfigSettings config = sslConfig.config();

    // -----------
    final char[] password = "abcdef".toCharArray();// do not store passwords in code, read them from somewhere safe!

    final KeyStore ks = KeyStore.getInstance("PKCS12");
    final InputStream keystore = TcpTLSEcho.class.getClassLoader().getResourceAsStream("keys/server.p12");

    ks.load(keystore, password);


    // initial ssl context
    final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
    tmf.init(ks);

    final KeyManagerFactoryWrapper keyManagerFactory = sslConfig.buildKeyManagerFactory(config);
    keyManagerFactory.init(ks, password);

    final SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagerFactory.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

    // protocols
    final SSLParameters defaultParams = sslContext.getDefaultSSLParameters();
    final String[] defaultProtocols = defaultParams.getProtocols();
    final String[] protocols = sslConfig.configureProtocols(defaultProtocols, config);
    defaultParams.setProtocols(protocols);

    // ciphers
    final String[] defaultCiphers = defaultParams.getCipherSuites();
    final String[] cipherSuites = sslConfig.configureCipherSuites(defaultCiphers, config);
    defaultParams.setCipherSuites(cipherSuites);

    TLSProtocol.NegotiateNewSession firstSession =
      TLSProtocol.negotiateNewSession()
        .withCipherSuites(cipherSuites)
        .withProtocols(protocols)
        .withParameters(defaultParams);

    // auth

    final Optional<TLSClientAuth> clientAuth = getClientAuth(config.sslParametersConfig().clientAuth());
    if (clientAuth.isPresent()) {
      firstSession = firstSession.withClientAuth(clientAuth.get());
    }

    final BidiFlow<TLSProtocol.SslTlsOutbound, ByteString, ByteString, TLSProtocol.SslTlsInbound, NotUsed> tls =
      TLS.create(sslContext, firstSession, role);

    final PartialFunction<TLSProtocol.SslTlsInbound, ByteString> pf =
      new PFBuilder()
        .match(TLSProtocol.SessionBytes.class, (sb) -> ((TLSProtocol.SessionBytes) sb).bytes())
        .match(TLSProtocol.SslTlsInbound.class, (ib) -> {
          system.log().info("Received other that SessionBytes" + ib);
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
    if (auth.equals(ClientAuth.want())) {
      return Optional.of(TLSClientAuth.want());
    } else if (auth.equals(ClientAuth.need())) {
      return Optional.of(TLSClientAuth.need());
    } else if (auth.equals(ClientAuth.none())) {
      return Optional.of(TLSClientAuth.none());
    } else {
      return Optional.empty();
    }
  }
  
}
