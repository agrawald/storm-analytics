package au.uni.melb.cloud.computing.analytics.domain;

public class VulgarSentiment extends Sentiment {
    private String vulgar;

    public VulgarSentiment(String sentiment, String vulgar) {
        super(sentiment);
        this.vulgar = vulgar;
    }

    public String getVulgar() {
        return vulgar;
    }
}
