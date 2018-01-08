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
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.utils.Time;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.mutable.Seq;

public class EmbeddedKafkaServer {
    KafkaServer kafkaServer;
    int port;
    ZkClient zkClient;
    ZkUtils zkUtils;

    public EmbeddedKafkaServer(KafkaServer kafkaServer, int port, ZkClient zkClient, ZkUtils zkUtils) {
        this.kafkaServer = kafkaServer;
        this.port = port;
        this.zkClient = zkClient;
        this.zkUtils = zkUtils;
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
        // see => https://github.com/apache/kafka/blob/1.0.0/core/src/test/scala/unit/kafka/utils/TestUtils.scala#L199-L215
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
                                1 // logDirCount
                        );

        ZkClient zkClient = new ZkClient(zkConnectionString, 6000, 10000, ZKStringSerializer$.MODULE$);

        ZkUtils zkUtils =
                ZkUtils
                        .apply(
                                zkClient,  // zkClient
                                false  // isZkSecurityEnabled
                        );

        KafkaServer kafkaServer = TestUtils.createServer(new KafkaConfig(properties), Time.SYSTEM);

        return new EmbeddedKafkaServer(kafkaServer, port, zkClient, zkUtils);
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

    public static void createTopic(String topic, int numPartitions, int replicationFactor, EmbeddedKafkaServer... servers) {
        createTopic(topic, numPartitions, replicationFactor, Arrays.asList(servers));
    }

    public static void createTopic(String topic, int numPartitions, int replicationFactor, List<EmbeddedKafkaServer> servers) {
        Seq<KafkaServer> kafkaServers =
                JavaConverters
                        .asScalaBuffer(
                                servers
                                        .stream()
                                        .map(s -> s.kafkaServer)
                                        .collect(Collectors.toList())
                        );

        ZkUtils zkUtils = servers.get(0).zkUtils;

        TestUtils.createTopic(zkUtils, topic, numPartitions, replicationFactor, kafkaServers, new Properties());
    }

    public void stop() {
        kafkaServer.shutdown();
        zkClient.close();
    }
}
