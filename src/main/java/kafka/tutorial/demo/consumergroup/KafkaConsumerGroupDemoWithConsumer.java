package kafka.tutorial.demo.consumergroup;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

public class KafkaConsumerGroupDemoWithConsumer {
    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "BVConsumerGroupTest5");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                StringDeserializer.class.getName());
        props.put("value.deserializer",
                StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);


        consumer.subscribe(Arrays.asList("multi_partition_topic"));


        int i = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);


            Set<TopicPartition> assignedPartitions = records.partitions();
            assignedPartitions.stream().forEach(System.out::println);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, key = %s, value = %s\n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
                Thread.sleep(1000);
            }


        }

    }
}
