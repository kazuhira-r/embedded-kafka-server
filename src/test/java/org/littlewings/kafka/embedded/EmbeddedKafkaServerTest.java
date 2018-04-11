package org.littlewings.kafka.embedded;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EmbeddedKafkaServerTest {
    @Test
    public void embeddedZooKeeperAndKafkaServer() {
        EmbeddedZookeeper zkServer = new EmbeddedZookeeper();

        String zkConnectionString = "localhost:" + zkServer.port();

        EmbeddedKafkaServer kafkaServer = EmbeddedKafkaServer.start(zkConnectionString);

        try {
            EmbeddedKafkaServer.createTopic(zkConnectionString, "test-topic", 1, 1, kafkaServer);

            String brokerConnectionString = kafkaServer.getBrokerConnectionString();

            Properties brokerProperties = new Properties();
            brokerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConnectionString);
            brokerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            brokerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(brokerProperties)) {
                producer.send(new ProducerRecord<>("test-topic", "key1", "value1"));
                producer.send(new ProducerRecord<>("test-topic", "key2", "value2"));
            }

            Properties consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConnectionString);
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

            try (KafkaConsumer<String, String> consumer =
                         new KafkaConsumer<>(consumerProperties)) {
                consumer.subscribe(Arrays.asList("test-topic"));

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(1000L);

                    if (records.isEmpty()) {
                        continue;
                    }

                    assertThat(records)
                            .hasSize(2);

                    Iterator<ConsumerRecord<String, String>> iterator = records.iterator();

                    ConsumerRecord<String, String> record1 = iterator.next();
                    assertThat(record1.key()).isEqualTo("key1");
                    assertThat(record1.value()).isEqualTo("value1");

                    ConsumerRecord<String, String> record2 = iterator.next();
                    assertThat(record2.key()).isEqualTo("key2");
                    assertThat(record2.value()).isEqualTo("value2");

                    break;
                }
            }
        } finally {
            kafkaServer.stop();
            zkServer.shutdown();
        }
    }

    @Test
    public void embeddedZooKeeperAndKafkaServerCluster() {
        EmbeddedZookeeper zkServer = new EmbeddedZookeeper();

        String zkConnectionString = "localhost:" + zkServer.port();

        List<EmbeddedKafkaServer> kafkaServers =
                IntStream
                        .rangeClosed(1, 3)
                        .mapToObj(i -> EmbeddedKafkaServer.start(i, zkConnectionString))
                        .collect(Collectors.toList());

        try {
            EmbeddedKafkaServer.createTopic(zkConnectionString, "test-topic", 3, 2, kafkaServers);

            String brokerConnectionString =
                    kafkaServers
                            .stream()
                            .map(EmbeddedKafkaServer::getBrokerConnectionString)
                            .collect(Collectors.joining(","));

            Properties brokerProperties = new Properties();
            brokerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConnectionString);
            brokerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            brokerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(brokerProperties)) {
                producer.send(new ProducerRecord<>("test-topic", "key1", "value1"));
                producer.send(new ProducerRecord<>("test-topic", "key2", "value2"));
            }

            Properties consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConnectionString);
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

            try (KafkaConsumer<String, String> consumer =
                         new KafkaConsumer<>(consumerProperties)) {
                consumer.subscribe(Arrays.asList("test-topic"));

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(1000L);

                    if (records.isEmpty()) {
                        continue;
                    }

                    assertThat(records)
                            .hasSize(2);

                    Iterator<ConsumerRecord<String, String>> iterator = records.iterator();

                    ConsumerRecord<String, String> record1 = iterator.next();
                    assertThat(record1.key()).isEqualTo("key1");
                    assertThat(record1.value()).isEqualTo("value1");

                    ConsumerRecord<String, String> record2 = iterator.next();
                    assertThat(record2.key()).isEqualTo("key2");
                    assertThat(record2.value()).isEqualTo("value2");

                    break;
                }
            }
        } finally {
            kafkaServers.forEach(EmbeddedKafkaServer::stop);
            zkServer.shutdown();
        }
    }
}
