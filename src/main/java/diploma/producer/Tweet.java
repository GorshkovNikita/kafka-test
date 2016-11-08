package diploma.producer;

import twitter4j.Status;

import java.util.Date;

/**
 * @author Никита
 */
public class Tweet {
    private Long id;
    private String text;
    private Date date;
    private String username;
    private String url;
    private Tweet retweetedStatus;

    public Tweet(Status status) {
        this.setId(status.getId());
        this.setDate(status.getCreatedAt());
        this.setText(status.getText());
        this.setUsername(status.getUser().getScreenName());
        this.setUrl("https://twitter.com/" + status.getUser().getScreenName() + "/status/" + status.getId());
        if (status.getRetweetedStatus() != null)
            this.setRetweetedStatus(new Tweet(status.getRetweetedStatus()));
        else this.setRetweetedStatus(null);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Tweet getRetweetedStatus() {
        return retweetedStatus;
    }

    public void setRetweetedStatus(Tweet retweetedStatus) {
        this.retweetedStatus = retweetedStatus;
    }
}
