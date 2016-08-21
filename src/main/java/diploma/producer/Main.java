package diploma.producer;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Properties;

/**
 * Created by Никита on 21.04.2016.
 */
public class Main {
    public static int i = 0;

    public static void main(String[] args) throws TwitterException, InterruptedException, IOException {
        Path jsonPath = Paths.get("D:\\MSU\\diploma\\json-tweets.txt");
        Path purePath = Paths.get("D:\\MSU\\diploma\\pure-tweets.txt");
        Integer rate;
        if (args.length < 1) {
            System.out.println("Arguments not found");
            return;
        }
        switch (args[0]) {
            case "kafka":
                if (args.length < 2) {
                    System.out.println("Rate not found");
                    return;
                }
                try {
                    rate = Integer.parseInt(args[1]);
                } catch (NumberFormatException ex) {
                    System.out.println(ex.getMessage());
                    return;
                }
                if (args.length == 3)
                    purePath = Paths.get(args[2]);
                sendFromFileToKafka(purePath, rate);
                break;
            case "file":
                if (args.length == 3) {
                    jsonPath = Paths.get(args[1]);
                    purePath = Paths.get(args[2]);
                }
                sendFromTwitterToFile(jsonPath, purePath);
                break;
            case "file-to-file":
                if (args.length == 3) {
                    jsonPath = Paths.get(args[1]);
                    purePath = Paths.get(args[2]);
                }
                sendFromFileToFile(jsonPath, purePath);
                break;
            case "delete":
                if (args.length == 3) {
                    jsonPath = Paths.get(args[1]);
                    purePath = Paths.get(args[2]);
                }
                deleteEmptyStrings(jsonPath, purePath);
                break;
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
                        if (line != null && !line.equals("")) {
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
                    if (line != null && !line.equals("")) {
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

    public static void sendFromTwitterToFile(Path jsonPath, Path purePath) throws IOException {
        TwitterStreamConnection.getInstance("YOcgp2ovL8js849lx8hbnvxcf",
                "IUxjGCksxWJiBlQ5PMsp5O8ksT7ZAsTDspOQafm46gSkYnII4u",
                "4482173056-cZrtVBDKyRoeciGNs0JaDBtaNgGEl1IHKIckeSI",
                "1nCVck1dtozb334vxlyca9Wb3Gq5ob7USXEX5sIqmIugs").getClient().connect();

        if (!Files.exists(jsonPath))
            Files.createFile(jsonPath);
        if (!Files.exists(purePath))
            Files.createFile(purePath);
        FileWriter jsonWriter = new FileWriter(jsonPath.toFile(), true);
        FileWriter pureWriter = new FileWriter(purePath.toFile(), true);
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
//                String text;
//                try {
//                    text = TwitterObjectFactory.createStatus(msg).getText();
//                }
//                catch (TwitterException ex) {
//                    text = "";
//                }
//                if (!text.equals("")) {
//                    text = StringEscapeUtils.escapeJava(text);
//                    pureWriter.append(text);
//                    pureWriter.append(System.getProperty("line.separator"));
//                }
            }
        }
        jsonWriter.close();
        pureWriter.close();
        TwitterStreamConnection.getInstance().getClient().stop();
    }

    public static void sendFromFileToFile(Path sourcePath, Path destinationPath) throws IOException {
        if (!Files.exists(destinationPath))
            Files.createFile(destinationPath);
        FileWriter tweetsWriter = new FileWriter(destinationPath.toFile(), true);
        int i = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(sourcePath.toString()))) {
            String line = null;
            do {
                line = br.readLine();
                if (line != null && !line.equals("")) {
                    line = StringEscapeUtils.escapeJava(line);
                    tweetsWriter.append(line);
                    tweetsWriter.append(System.getProperty("line.separator"));
                }
            } while (line != null);
            br.close();
        }
        catch (IOException | IllegalArgumentException ex) {
            ex.printStackTrace();
        }
        tweetsWriter.close();
    }

    public static void deleteEmptyStrings(Path sourcePath, Path destinationPath) throws IOException {
        if (!Files.exists(sourcePath))
            return;
        if (!Files.exists(destinationPath))
            Files.createFile(destinationPath);
        FileWriter tweetsWriter = new FileWriter(destinationPath.toFile());
        try (BufferedReader br = new BufferedReader(new FileReader(sourcePath.toString()))) {
            String line = null;
            do {
                line = br.readLine();
                if (line != null && !line.equals("")) {
                    tweetsWriter.append(line);
                    tweetsWriter.append(System.getProperty("line.separator"));
                }
            } while (line != null);
            br.close();
        }
        catch (IOException | IllegalArgumentException ex) {
            ex.printStackTrace();
        }
        tweetsWriter.close();
    }

    public static int getNextInt() {
        return ++i;
    }

    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.KAFKA_BROKER_LIST);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 8192);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }
}
