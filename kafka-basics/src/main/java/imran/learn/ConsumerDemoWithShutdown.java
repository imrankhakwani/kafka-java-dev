package imran.learn;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Consumer demo.");

        String groupId = "temperature-readings";
        String topic = "temperature";

        Properties properties = new Properties();

        // Bootstrap server (kafka broker)
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Consumer configuration parameters
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        // none | earliest | latest
        // none : assumes the group id is created before running the consumer application
        // earliest : reads the messages from beginning
        // latest : only interested in messages sent after running the application
        properties.setProperty("auto.offset.reset", "earliest");

        // create consumer objects and set the properties
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Get reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // Add the shutdown hook to runtime
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("A shutdown event is detected. Shutting down by calling consumer.wakeup()...");
                consumer.wakeup();

                // Join the main thread to allow the execution of main thread.
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            // subscribe to topic
            consumer.subscribe(Arrays.asList(topic));

            // poll data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Value: " + record.offset());
                }
            }
        } catch(WakeupException we) {
            log.info("Consumer is shutting down...");
        } catch(Exception e) {
            log.error("Unexpected exception encountered.", e);
        } finally {
            // Close the consumer, this will commit offsets as well.
            consumer.close();
            log.info("The consumer is now gracefully shutdown.");
        }
    }
}
