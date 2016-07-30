package diploma.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.TwitterException;

import java.io.*;
import java.nio.file.*;
import java.util.Properties;

/**
 * Created by Никита on 21.04.2016.
 */
public class Main {
    public static int i = 0;

    public static void main(String[] args) throws TwitterException, InterruptedException, IOException {
        Path path = Paths.get("/home/ec2-user/diploma/tweets/json-tweets.txt");
        Integer rate;
        if (args.length < 1) {
            System.out.println("Arguments not found");
            return;
        }

        if (args[0].equals("kafka")) {
            if (args.length < 2) {
                System.out.println("Rate not found");
                return;
            }
            try {
                rate = Integer.parseInt(args[1]);
            }
            catch (NumberFormatException ex) {
                System.out.println(ex.getMessage());
                return;
            }
            if (args.length == 3)
                path = Paths.get(args[2]);
            sendFromFileToKafka(path, rate);
        }
        else if (args[0].equals("file")) {
            if (args.length == 2)
                path = Paths.get(args[1]);
            sendFromTwitterToFile(path);
        }
    }

    public static void sendFromFileToKafka(Path path, Integer rate) throws IOException {
        Producer<String, String> producer = createProducer();
        try (BufferedReader br = new BufferedReader(new FileReader(path.toString()))) {
            String line = null;
            long globalStart = System.currentTimeMillis();
            if (rate != 0) {
                Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
                do {
                    long start = System.currentTimeMillis();
                    long sendTime = 0;
                    for (int i = 0; i < rate; i++) {
                        line = br.readLine();
                        if (line != null) {
                            long startSendTime = System.currentTimeMillis();
                            producer.send(new ProducerRecord<>(Config.KAFKA_TOPIC, Integer.toString(getNextInt()), line));
                            sendTime += System.currentTimeMillis() - startSendTime;
                        } else break;
                    }
                    System.gc();
                    long finish = System.currentTimeMillis() - start;
                    long sleepTime = 1000 - finish;
                    System.out.println("sendTime = " + sendTime);
                    if (sleepTime > 0)
                        Thread.sleep(sleepTime);
                    else
                        System.out.println(sleepTime + " is negative");
                } while (line != null);
            }
            else {
                System.out.println("I am reading with max rate");
                do {
                    line = br.readLine();
                    if (line != null) {
                        producer.send(new ProducerRecord<>(Config.KAFKA_TOPIC, Integer.toString(getNextInt()), line));
                    }
                } while (line != null);
            }
            long globalFinish = System.currentTimeMillis() - globalStart;
            System.out.println(globalFinish / 1000 + " seconds");
            br.close();
        }
        catch (InterruptedException | IOException | IllegalArgumentException ex) {
            ex.printStackTrace();
        }
        producer.close();
    }

    public static void sendFromTwitterToFile(Path path) throws IOException {
        TwitterStreamConnection.getInstance("YOcgp2ovL8js849lx8hbnvxcf",
                "IUxjGCksxWJiBlQ5PMsp5O8ksT7ZAsTDspOQafm46gSkYnII4u",
                "4482173056-cZrtVBDKyRoeciGNs0JaDBtaNgGEl1IHKIckeSI",
                "1nCVck1dtozb334vxlyca9Wb3Gq5ob7USXEX5sIqmIugs").getClient().connect();

        if (!Files.exists(path))
            Files.createFile(path);
        FileWriter jsonWriter = new FileWriter(path.toFile(), true);
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
                jsonWriter.append(msg);
                if (i == 100000)
                    break;
            }
        }
        jsonWriter.close();
        TwitterStreamConnection.getInstance().getClient().stop();
    }

    public static int getNextInt() {
        return ++i;
    }

    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.KAFKA_BROKER_LIST);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }
}
