package diploma.producer;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import javax.xml.soap.Text;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Никита on 21.04.2016.
 */
public class Main {
    public static int i = 0;

    public static void main(String[] args) throws TwitterException, InterruptedException, IOException {
        // TODO: убрать дефолтные названия и добавить эксепшены, если количество входных данных неверно
        Path jsonPath = Paths.get("D:\\MSU\\diploma\\json-tweets.txt");
        Path purePath = Paths.get("D:\\MSU\\diploma\\pure-tweets.txt");
        Integer rate;
        if (args.length < 1) {
            System.out.println("Arguments not found");
            return;
        }
        switch (args[0]) {
            case "top-terms":
                findTopTerms();
                break;
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
            case "transform":
                if (args.length == 3) {
                    jsonPath = Paths.get(args[1]);
                    purePath = Paths.get(args[2]);
                }
                transformTweets(jsonPath, purePath);
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

    private static void findTopTerms() {
        TwitterStreamConnection.getInstance("YOcgp2ovL8js849lx8hbnvxcf",
                "IUxjGCksxWJiBlQ5PMsp5O8ksT7ZAsTDspOQafm46gSkYnII4u",
                "4482173056-cZrtVBDKyRoeciGNs0JaDBtaNgGEl1IHKIckeSI",
                "1nCVck1dtozb334vxlyca9Wb3Gq5ob7USXEX5sIqmIugs").getClient().connect();

        Map<String, Integer> topTerms = new HashMap<>();
        while (true) {
            if (TwitterStreamConnection.getInstance().getClient().isDone()) {
                System.out.println("Client connection closed unexpectedly: " + TwitterStreamConnection.getInstance().getClient().getExitEvent().getMessage());
                break;
            }

            String msg = TwitterStreamConnection.getNextMessage();
            if (msg == null) {
                System.out.println("Did not receive a message in 1 second");
            } else {
                try {
                    Status status = TwitterObjectFactory.createStatus(msg);
                    String[] terms = TextNormalizer.getInstance().normalizeToString(status.getText()).split(" ");
                    for (String term: terms) {
                        if (!topTerms.containsKey(term)) topTerms.put(term, 1);
                        else topTerms.put(term, topTerms.get(term) + 1);
                    }
                    i++;
                }
                catch (TwitterException ex) {
                    System.err.println("can not create tweet for json = " + msg);
                }
            }
        }
        TwitterStreamConnection.getInstance().getClient().stop();
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
                        }
                        else if (line != null && line.equals("")) {
                            while (line.equals("")) {
                                line = br.readLine();
                            }
                        }
                        else break;
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
//        TextNormalizer textNormalizer = new TextNormalizer();

        if (!Files.exists(jsonPath))
            Files.createFile(jsonPath);
        if (!Files.exists(purePath))
            Files.createFile(purePath);
        FileWriter jsonWriter = new FileWriter(jsonPath.toFile(), true);
//        FileWriter pureWriter = new FileWriter(purePath.toFile(), true);
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
                try {
                    Status status = TwitterObjectFactory.createStatus(msg);
                    Tweet tweet = new Tweet(status);
                    System.out.println(tweet.getDate().toString() + " " + tweet.getText());
                    jsonWriter.append(msg);
//                    Gson gson = new Gson();
//                    String json = gson.toJson(tweet);
//                    pureWriter.append(json + "\n");
                    i++;
                }
                catch (TwitterException ex) {
                    System.err.println("can not create tweet for json = " + msg);
                }
            }
        }
        jsonWriter.close();
//        pureWriter.close();
        TwitterStreamConnection.getInstance().getClient().stop();
    }

    /**
     * Преобразование твитов, собранных мной в формат, необходимый для работы программы online-denstream
     * @param sourcePath - файл с моими твитами
     * @param destinationPath - файл в новом формате
     * @throws IOException
     */
    public static void transformTweets(Path sourcePath, Path destinationPath) throws IOException {
        if (!Files.exists(destinationPath))
            Files.createFile(destinationPath);
        FileWriter tweetsWriter = new FileWriter(destinationPath.toFile(), true);
        try (BufferedReader br = new BufferedReader(new FileReader(sourcePath.toString()))) {
            String line = null;
            do {
                line = br.readLine();
                if (line != null && !line.equals("")) {
                    try {
                        Status status = TwitterObjectFactory.createStatus(line);
                        tweetsWriter.append(Long.toString(status.getId()))
                                .append('\t')
                                .append(status.getCreatedAt().toString())
                                .append('\t')
                                .append(StringEscapeUtils.escapeJava(status.getText()))
                                .append('\t')
                                .append(Integer.toString(status.getRetweetCount()))
                                .append('\t');
                        if (status.getGeoLocation() != null) {
                            tweetsWriter.append(Double.toString(status.getGeoLocation().getLatitude()))
                                    .append(",")
                                    .append(Double.toString(status.getGeoLocation().getLongitude()));
                        }
                        tweetsWriter.append(System.getProperty("line.separator"));
                    }
                    catch (TwitterException ex) {
                        System.err.println("can not create tweet for json = " + line);
                    }
                }
            } while (line != null);
            br.close();
            tweetsWriter.close();
        }
        catch (IOException | IllegalArgumentException ex) {
            ex.printStackTrace();
        }
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
                    String[] lines = line.split("=");
                    line = StringEscapeUtils.escapeJava(lines[1]);
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
