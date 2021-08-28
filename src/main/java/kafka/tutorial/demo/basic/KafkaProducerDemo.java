package kafka.tutorial.demo.basic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerDemo {
    public static void main(String[] args) throws InterruptedException {


        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9091,localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        int i=1;

        while(true) {
            String message="Message "+i;

            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>("first_topic", message);

            producer.send(producerRecord);

            System.out.println("Sending message "+message);
            i++;
            Thread.sleep(3000);
        }



    }
}
