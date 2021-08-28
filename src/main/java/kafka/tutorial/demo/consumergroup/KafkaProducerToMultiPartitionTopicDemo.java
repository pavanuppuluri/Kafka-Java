package kafka.tutorial.demo.consumergroup;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerToMultiPartitionTopicDemo {
    public static void main(String[] args) throws InterruptedException {


        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        int i=1;

        while(i<=1000) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("multi_partition_topic", "Message " + i++);
            producer.send(producerRecord);
            producer.flush();
            Thread.sleep(1000);
        }
        producer.close();
    }
}