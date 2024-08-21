package demos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());


    public void producerWithKeys() {

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j = 0; j < 2; j++) {

            for (int i = 0; i < 10; i++) {

                String topic = "demo_java";
                String key = "id " + i;
                String value = "Hello world" + i;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            log.info(
                                    "Key: " + key + " " + "Partition: " + metadata.partition());
                        } else {
                            log.error("Error", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


        producer.flush();

        producer.close();
    }


    public static void main(String[] args) {
        ProducerDemoKeys p = new ProducerDemoKeys();

        p.producerWithKeys();
    }



}
