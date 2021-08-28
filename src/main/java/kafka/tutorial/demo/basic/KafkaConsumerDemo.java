package kafka.tutorial.demo.basic;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        props.put("group.id", "BVConsumerGroup");

        props.put("key.deserializer",
                StringDeserializer.class.getName());
        props.put("value.deserializer",
                StringDeserializer.class.getName());
        props.put("auto.offset.reset","latest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList("first_topic"));

        int i = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
            // Commit
        }

    }
}
