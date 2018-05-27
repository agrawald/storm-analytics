package au.uni.melb.cloud.computing.analytics.domain;

public class Sentiment extends Estimation {
    private String sentiment;

    public Sentiment(String sentiment) {
        this.sentiment = sentiment;
    }

    public String getSentiment() {
        return sentiment;
    }
}
