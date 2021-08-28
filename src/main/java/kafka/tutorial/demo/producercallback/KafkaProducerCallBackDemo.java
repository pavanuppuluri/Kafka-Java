package kafka.tutorial.demo.producercallback;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerCallBackDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {


        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("delivery.timeout.ms", 9000);
        props.put("request.timeout.ms", 3000);
        props.put("linger.ms", 3000);



        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for(int i=0;i<100;i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("producer_callback", "Message "+i);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        System.out.println("Exception while sending data " + e.getMessage());
                    } else {
                        System.out.println("Message sent successfully");
                        System.out.println(recordMetadata.topic() + " : " + recordMetadata.partition()
                                + " : " + recordMetadata.offset());

                    }

                }
            });


            producer.flush();
            Thread.sleep(1000);
        }
        producer.close();

    }
}
