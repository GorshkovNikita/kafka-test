package diploma.producer;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * Created by Никита on 19.06.2016.
 */
public class TwitterQueueRestClient {
    private static CloseableHttpClient httpclient;

    static {
        httpclient = HttpClients.createDefault();
    }

    public static String nextBatch(int count) throws IOException {
        HttpGet httpGet = new HttpGet("http://192.168.1.21:7070/queue?count=" + count);
        CloseableHttpResponse response = httpclient.execute(httpGet);
        String responseBody = EntityUtils.toString(response.getEntity());
        return !responseBody.equals("empty") ? responseBody : null;
    }

    public static void main(String[] args) throws IOException {
        String str1 = nextBatch(1000);
        String str2 = nextBatch(1000);
        System.out.println(str1);
        System.out.println(str2);
    }
}
