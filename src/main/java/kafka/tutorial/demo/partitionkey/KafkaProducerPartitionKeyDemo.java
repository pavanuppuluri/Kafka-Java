package kafka.tutorial.demo.partitionkey;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerPartitionKeyDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {


        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);





        for (int i = 1; i <= 100; i++) {
            String key=getKey();
            String message="Message "+i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("multi_partition_topic", key, message);

            Future<RecordMetadata> sendResult = producer.send(producerRecord);
            RecordMetadata recordMetadata = sendResult.get();
            System.out.print("Message with Key " + key);
            System.out.println(" ===> Assigned to Partition " + recordMetadata.partition());
            Thread.sleep(1000);

        }


        producer.flush();
        producer.close();

    }

    public static String getKey() {
        String[] key={"IN","US","CN","UK","BR","CA"};
        // create Random object
        Random random = new Random();
        // generate random number from 0 to 5
        int number = random.nextInt(6);
        return key[number];
    }
}
