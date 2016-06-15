package diploma.producer;

import com.twitter.hbc.core.HttpConstants;
import com.twitter.hbc.core.endpoint.DefaultStreamingEndpoint;

/**
 * Created by Никита on 15.06.2016.
 */
public class StatusesEndpoint extends DefaultStreamingEndpoint {

    public static final String PATH = "/statuses/sample.json";

    public StatusesEndpoint() {
        super(PATH, HttpConstants.HTTP_POST, false);
    }
}
