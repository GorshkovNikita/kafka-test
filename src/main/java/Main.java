import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.util.Properties;

/**
 * Created by Никита on 21.04.2016.
 */
public class Main {
    public static void main(String[] args) throws TwitterException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //Status status = TwitterObjectFactory.createStatus("{text: \"asdad\"}");
        Producer<String, String> producer = new KafkaProducer<>(props);
        for(int i = 0; true ; i++) {
            producer.send(new ProducerRecord<>("my-replicated-topic", Integer.toString(i), "status" + i));
            //Thread.sleep(2000);
        }

        //producer.close();
    }
}
