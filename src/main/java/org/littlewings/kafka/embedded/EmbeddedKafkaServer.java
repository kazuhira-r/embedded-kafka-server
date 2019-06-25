package org.littlewings.kafka.embedded;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.KafkaZkClient;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.utils.Time;
import scala.Int;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.mutable.Seq;

public class EmbeddedKafkaServer {
    KafkaServer kafkaServer;
    int port;
    ZkClient zkClient;

    public EmbeddedKafkaServer(KafkaServer kafkaServer, int port, ZkClient zkClient) {
        this.kafkaServer = kafkaServer;
        this.port = port;
        this.zkClient = zkClient;
    }

    static int randomPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static EmbeddedKafkaServer start(String zkConnectionString) {
        return start(1, zkConnectionString, randomPort());
    }

    public static EmbeddedKafkaServer start(int brokerId, String zkConnectionString) {
        return start(brokerId, zkConnectionString, randomPort());
    }

    public static EmbeddedKafkaServer start(int brokerId, String zkConnectionString, int port) {
        // TestUtils#createBrokerConfig defalut values
        // see => https://github.com/apache/kafka/blob/2.0.0/core/src/test/scala/unit/kafka/utils/TestUtils.scala#L199-L216
        Properties properties =
                TestUtils
                        .createBrokerConfig(
                                brokerId,  // nodeId
                                zkConnectionString,  // zkConnect
                                true,  //     enableControlledShutdown
                                false,  // enableDeleteTopic
                                port, // port
                                Option.empty(), // interBrokerSecurityProtocol
                                Option.empty(),  // trustStoreFile
                                Option.empty(),  // saslProperties
                                true, // enablePlaintext
                                false,  // enableSaslPlaintext
                                0, // saslPlaintextPort
                                false,  // enableSsl
                                0,  // sslPort
                                false,  // enableSaslSsl
                                0,  // saslSslPort
                                Option.empty(),  // rack
                                1, // logDirCount
                                false // enableToken
                        );

        ZkClient zkClient = new ZkClient(zkConnectionString, 6000, 10000, ZKStringSerializer$.MODULE$);

        KafkaServer kafkaServer = TestUtils.createServer(new KafkaConfig(properties), Time.SYSTEM);

        return new EmbeddedKafkaServer(kafkaServer, port, zkClient);
    }

    public int getBrokerId() {
        return kafkaServer.config().brokerId();
    }

    public int getPort() {
        return port;
    }

    public String getBrokerConnectionString() {
        return String.format("localhost:%d", getPort());
    }

    public static void createTopic(String zkConnectionString, String topic, int numPartitions, int replicationFactor, EmbeddedKafkaServer... servers) {
        createTopic(zkConnectionString, topic, numPartitions, replicationFactor, Arrays.asList(servers));
    }

    public static void createTopic(String zkConnectionString, String topic, int numPartitions, int replicationFactor, List<EmbeddedKafkaServer> servers) {
        Seq<KafkaServer> kafkaServers =
                JavaConverters
                        .asScalaBuffer(
                                servers
                                        .stream()
                                        .map(s -> s.kafkaServer)
                                        .collect(Collectors.toList())
                        );

        // see => https://github.com/apache/kafka/blob/2.3.0/core/src/test/scala/unit/kafka/zk/ZooKeeperTestHarness.scala#L39-L61
	// see => https://github.com/apache/kafka/blob/2.3.0/core/src/main/scala/kafka/zk/KafkaZkClient.scala#L1816-L1824
        KafkaZkClient zkClient =
                KafkaZkClient
                        .apply(
                                zkConnectionString,
                                false,
                                15000,
                                10000,
                                Int.MaxValue(),
                                Time.SYSTEM,
                                "kafka.server",
                                "SessionExpireListener",
				Option.empty()
                        );

        TestUtils.createTopic(zkClient, topic, numPartitions, replicationFactor, kafkaServers, new Properties());
    }

    public void stop() {
        kafkaServer.shutdown();
        zkClient.close();
    }
}
