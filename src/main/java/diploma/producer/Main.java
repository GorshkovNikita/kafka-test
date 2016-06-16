package diploma.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.metrics.Stat;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Properties;
import java.util.Random;

/**
 * Created by Никита on 21.04.2016.
 */
public class Main {
    public static void main(String[] args) throws TwitterException, InterruptedException, FileNotFoundException, UnsupportedEncodingException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.23:9092, 192.168.1.22:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //Producer<String, String> producer = new KafkaProducer<>(props);

        TwitterStreamConnection.getInstance("YOcgp2ovL8js849lx8hbnvxcf",
                "IUxjGCksxWJiBlQ5PMsp5O8ksT7ZAsTDspOQafm46gSkYnII4u",
                "4482173056-cZrtVBDKyRoeciGNs0JaDBtaNgGEl1IHKIckeSI",
                "1nCVck1dtozb334vxlyca9Wb3Gq5ob7USXEX5sIqmIugs").getClient().connect();

        Random random = new Random();
        PrintWriter jsonWriter = new PrintWriter("~/diploma/tweets/json-tweets.txt", "UTF-8");
        //PrintWriter textWriter = new PrintWriter("~/diploma/tweets/text-tweets.txt", "UTF-8");
        int i = 0;
        while (true) {
            if (TwitterStreamConnection.getInstance().getClient().isDone()) {
                System.out.println("Client connection closed unexpectedly: " + TwitterStreamConnection.getInstance().getClient().getExitEvent().getMessage());
                break;
            }

            String msg = TwitterStreamConnection.getNextMessage();
            if (msg == null) {
                System.out.println("Did not receive a message in 1 second");
            } else {
                i++;
                jsonWriter.write(msg);
                //producer.send(new ProducerRecord<>("my-replicated-topic", Integer.toString(i), msg));
                if (i == 500000)
                    break;
//                try {
//                    Status status = TwitterObjectFactory.createStatus(msg);
//                    System.out.println(status.getUser().getName() + " posted " + status.getText());
//                }
//                catch (TwitterException ex) {
//                    // cannot parse json, just ignore it
//                }
            }
        }

        jsonWriter.close();

        //for(int i = 0; i < 500; i++) {
            //producer.send(new ProducerRecord<>("my-replicated-topic", Integer.toString(i), "{\"text\": \"status" + i+ "\"}"));
            //producer.send(new ProducerRecord<>("my-replicated-topic", Integer.toString(i), TwitterStreamConnection.getNextMessage()));
            //Thread.sleep(250);
        //}

        TwitterStreamConnection.getInstance().getClient().stop();
        //producer.close();
    }
}
