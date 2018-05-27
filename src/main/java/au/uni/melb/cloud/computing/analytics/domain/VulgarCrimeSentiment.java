package au.uni.melb.cloud.computing.analytics.domain;

public class VulgarCrimeSentiment extends VulgarSentiment {
    private String name;
    private String regionId;
    private String region;

    public VulgarCrimeSentiment(String sentiment, String vulgar, String regionId, String region) {
        super(sentiment, vulgar);
        this.region = region;
        this.name = "sentiment-vulgar-crime";
        this.regionId = regionId;
    }
}
