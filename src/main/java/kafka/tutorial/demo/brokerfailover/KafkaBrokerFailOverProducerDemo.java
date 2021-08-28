package kafka.tutorial.demo.brokerfailover;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public class KafkaBrokerFailOverProducerDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {



        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        int i=0;
        while(i<100) {
            String message="Message "+i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("receipt_info_topic", "ID" + i, message);

            Future<RecordMetadata> sendResult = producer.send(producerRecord);
            RecordMetadata recordMetadata = sendResult.get();
            System.out.printf("Partition Key %d assigned to %d", i, recordMetadata.partition());
            System.out.println();
            i++;
            Thread.sleep(1000);


        }

        producer.flush();
        producer.close();

    }
}
