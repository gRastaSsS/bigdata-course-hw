package bigdata;

import common.Constants;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public final class BDResultConsumer {
    private BDResultConsumer() {
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "BDResultKafkaConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (Consumer<Void, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(Constants.OUT_TOPIC));

            while (true) {
                ConsumerRecords<Void, String> records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(record -> System.out.println(record.value()));
                consumer.commitAsync();
            }
        }
    }
}
