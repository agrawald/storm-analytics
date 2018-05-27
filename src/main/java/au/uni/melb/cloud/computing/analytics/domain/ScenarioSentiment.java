package au.uni.melb.cloud.computing.analytics.domain;

public class ScenarioSentiment extends Sentiment {
    private String name;
    private String region;
    private String regionId;

    public ScenarioSentiment(String sentiment, String name, String region, String regionId) {
        super(sentiment);
        this.name = name;
        this.region = region;
        this.regionId = regionId;
    }
}
