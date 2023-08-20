package imran.learn;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Producer demo with callbacks.");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Batch size only for demo, default batch size of 16k is for production workloads.
        properties.setProperty("batch.size", "400");

        // This setting is for demo, not suitable for production environments.
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("temperature", "Hello World " + i);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            log.info("\nReceived new metadata \n" +
                                    "Topic:" + metadata.topic() + "\n" +
                                    "Partition:" + metadata.partition() + "\n" +
                                    "Offset:" + metadata.offset() + "\n" +
                                    "Timestamp:" + metadata.timestamp() + "\n"
                            );
                        } else {
                            log.error("Error while producing: ", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.flush();
        producer.close();

    }
}
