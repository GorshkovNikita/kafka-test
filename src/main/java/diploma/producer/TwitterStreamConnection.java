package diploma.producer;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Никита on 03.04.2016.
 */ 
public class TwitterStreamConnection {
    private static TwitterStreamConnection instance;
    private BlockingQueue<String> messageQueue;
    private BasicClient client;

    private TwitterStreamConnection(String consumerKey, String consumerSecret, String token, String secret) {
        this.messageQueue = new LinkedBlockingQueue<>(10000);
//        StatusesEndpoint endpoint = new StatusesEndpoint();
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.languages(new ArrayList<>(Arrays.asList("en")));
        endpoint.trackTerms(new ArrayList<>(Arrays.asList("football", "ucl", "uefa", "Champions League")));
        endpoint.stallWarnings(false);
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        this.client = new ClientBuilder()
                .name("sampleExampleClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(messageQueue))
                .build();
    }

    public static String getNextMessage() {
        try {
            if (instance == null)
                throw new RuntimeException("Singleton has to be created");
            return instance.messageQueue.poll(1, TimeUnit.SECONDS);
        }
        catch (InterruptedException ex) {
            return null;
        }
    }

    public static TwitterStreamConnection getInstance(String consumerKey, String consumerSecret, String token, String secret) throws RuntimeException {
        if (instance != null)
            throw new RuntimeException("Singleton has been already created");
        instance = new TwitterStreamConnection(consumerKey, consumerSecret, token, secret);
        return instance;
    }

    public static TwitterStreamConnection getInstance() throws RuntimeException {
        if (instance == null)
            throw new RuntimeException("Singleton has to be created");
        return instance;
    }

    public BlockingQueue<String> getMessageQueue() {
        return messageQueue;
    }

    public BasicClient getClient() {
        return client;
    }
}
