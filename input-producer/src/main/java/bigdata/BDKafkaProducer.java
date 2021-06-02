package bigdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

public class BDKafkaProducer {
    private static final String TOPIC = "bigdata-input-topic";

    interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {
    }

    private BDKafkaProducer() {
    }

    public static void main(String[] args) throws Exception {
        Integer interval = Integer.parseInt(args[0]);
        String dir = args[1];
        String bootstrapServer = args[2];

        File dirFile = new File(dir);

        Random random = new Random();
        Producer<Long, String> producer = createProducer(bootstrapServer);

        for (String path : dirFile.list()) {
            try (CloseableIterator<JsonNode> iterator = readPosts(new File(dir + "/" + path))) {
                while (iterator.hasNext()){
                    Thread.sleep(random.nextInt(interval));

                    JsonNode post = iterator.next();
                    JsonNode postIdField = post.get("id");

                    if (postIdField != null) {
                        Long id = postIdField.asLong();
                        ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, id, post.toString());
                        producer.send(record).get();
                        System.out.println("Message " + id + " sent!");
                    }
                }

            }
        }

        producer.flush();
        producer.close();
    }

    private static CloseableIterator<JsonNode> readPosts(File postsFile) throws IOException {
        return new CloseableIterator<>() {
            private final ObjectMapper mapper = new ObjectMapper();
            private final BufferedReader br = new BufferedReader(new FileReader(postsFile));
            private String line = br.readLine();

            @Override
            public void close() throws Exception { br.close(); }

            @Override
            public boolean hasNext() { return line != null; }

            @Override
            public JsonNode next() {
                try {
                    JsonNode node = mapper.readTree(line);
                    line = br.readLine();
                    return node;
                } catch (IOException e) {
                    e.printStackTrace();
                    line = null;
                    return null;
                }
            }
        };
    }

    private static Producer<Long, String> createProducer(String bootstrapServer) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "BDKafkaProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
