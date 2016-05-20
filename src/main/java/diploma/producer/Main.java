package diploma.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.metrics.Stat;
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
        //props.put("metadata.broker.list", "fedora-0:9094,fedora-1:9092,fedora-1:9093");
	props.put("bootstrap.servers", "fedora-0:9092,fedora-2:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        TwitterStreamConnection.getInstance("YOcgp2ovL8js849lx8hbnvxcf",
                "IUxjGCksxWJiBlQ5PMsp5O8ksT7ZAsTDspOQafm46gSkYnII4u",
                "4482173056-cZrtVBDKyRoeciGNs0JaDBtaNgGEl1IHKIckeSI",
                "1nCVck1dtozb334vxlyca9Wb3Gq5ob7USXEX5sIqmIugs").getClient().connect();

        Status status = TwitterObjectFactory.createStatus("{\"text\": \"status" + 0 + "\"}");
//        Status status = TwitterObjectFactory.createStatus("{text: \"asdad\"}");
        Producer<String, String> producer = new KafkaProducer<>(props);
        for(int i = 0; true ; i++) {
            producer.send(new ProducerRecord<>("my-replicated-topic", Integer.toString(i), "{\"text\": \"status" + i + "\"}"));
            //producer.send(new ProducerRecord<>("my-replicated-topic", Integer.toString(i), TwitterStreamConnection.getNextMessage()));
            //Thread.sleep(2000);
        }

        //producer.close();
    }
}
