package demos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public void consumerDemo() {

        String groupId = "my-java-application";
        String topic = "demo_java";

        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        // Create a consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // subscribe to a topic
        kafkaConsumer.subscribe(Arrays.asList(topic));

        // poll for data
        while(true) {
            log.info("Polling");

            ConsumerRecords<String, String> consumerRecord = kafkaConsumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record : consumerRecord) {
                log.info("Key " + record.key() + "Value " + record.value());
                log.info("Partition " + record.partition() + "Offset " + record.offset());
            }
        }

    }


public static void main(String[] args) {
    ConsumerDemo c = new ConsumerDemo();
    c.consumerDemo();
}

}
