package au.uni.melb.cloud.computing.analytics.config;

import au.uni.melb.cloud.computing.analytics.bolt.*;
import au.uni.melb.cloud.computing.analytics.spout.TweeterSpout;
import au.uni.melb.cloud.computing.analytics.utils.Constants;
import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public enum TopologyConfig {
    INSTANCE;

    public TopologyBuilder initIncomeWklyTopology() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TweeterSpout.class.getSimpleName(), new TweeterSpout(), 1);
        builder.setBolt("s1Identifier", new SentimentBolt(), 1)
                .shuffleGrouping(TweeterSpout.class.getSimpleName());

        final RegionIdentifierBolt bIncomeWkly = new RegionIdentifierBolt(Constants.DB_INCOME_WKLY.getName());
        builder.setBolt("irw1Identifier", bIncomeWkly, 1)
                .setNumTasks(2)
                .shuffleGrouping("s1Identifier");

        builder.setBolt("irw2Counter",
                new IncomeWklySentimentCounterBolt(), 1)
                .fieldsGrouping("irw1Identifier",
                        new Fields(Constants.F_SENTIMENT.getName(), Constants.F_REGION.getName()));

        final RabbitPublisherBolt bIncomeWklySentimentPublisher = new RabbitPublisherBolt(Constants.Q_SENTIMENT_INCOME_WKLY.getName());
        builder.setBolt("irw3Publisher", bIncomeWklySentimentPublisher, 1)
                .setNumTasks(2)
                .shuffleGrouping("irw2Counter");
        return builder;
    }

    public TopologyBuilder initGreenPlacesTopology() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TweeterSpout.class.getSimpleName(), new TweeterSpout(), 1);
        builder.setBolt("s1Identifier", new SentimentBolt(), 1)
                .shuffleGrouping(TweeterSpout.class.getSimpleName());

        final RegionIdentifierBolt bGreenPlacesVisit = new RegionIdentifierBolt(Constants.DB_GREEN_PLACES.getName());
        builder.setBolt("gp1RegionIdentifier", bGreenPlacesVisit, 1)
                .setNumTasks(2)
                .shuffleGrouping("s1Identifier");

        builder.setBolt("gp2Counter", new GreenPlacesSentimentCounterBolt(), 1)
                .fieldsGrouping("gp1RegionIdentifier", new Fields(Constants.F_SENTIMENT.getName(), Constants.F_REGION.getName()));

        final RabbitPublisherBolt bGreenPlacesVisitSentimentPublisher = new RabbitPublisherBolt(Constants.Q_SENTIMENT_GREEN_PLACES_VISIT.getName());
        builder.setBolt("gp3Publisher", bGreenPlacesVisitSentimentPublisher, 1)
                .setNumTasks(2)
                .shuffleGrouping("gp2Counter");
        return builder;
    }

    public TopologyBuilder initCrimeTopology() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TweeterSpout.class.getSimpleName(), new TweeterSpout(), 1);
        builder.setBolt("s1Identifier", new SentimentBolt(), 1)
                .shuffleGrouping(TweeterSpout.class.getSimpleName());
        builder.setBolt("v1Identifier", new VulgarBolt(), 1)
                .fieldsGrouping("s1Identifier", new Fields(Constants.F_SENTIMENT.getName()));

        final RegionIdentifierBolt bCrimeBolt = new VulgarCrimeRegionIdentifierBolt(Constants.DB_CRIME_DATA.getName());
        builder.setBolt("vc1RegionIdentifier", bCrimeBolt, 1)
                .setNumTasks(2)
                .shuffleGrouping("v1Identifier");

        builder.setBolt("vc2Counter", new VulgarCrimeSentimentCounterBolt(), 1)
                .fieldsGrouping("vc1RegionIdentifier", new Fields(Constants.F_SENTIMENT.getName(), Constants.F_REGION.getName()));

        final RabbitPublisherBolt bCrimeSentimentPublisher = new RabbitPublisherBolt(Constants.Q_SENTIMENT_CRIME.getName());
        builder.setBolt("vc3Publisher", bCrimeSentimentPublisher, 1)
                .setNumTasks(2)
                .shuffleGrouping("vc2Counter");
        return builder;
    }

    public TopologyBuilder initVulgarTopology() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TweeterSpout.class.getSimpleName(), new TweeterSpout(), 1);
        builder.setBolt("s1Identifier", new SentimentBolt(), 1)
                .shuffleGrouping(TweeterSpout.class.getSimpleName());
        builder.setBolt("v1Identifier", new VulgarBolt(), 1)
                .fieldsGrouping("s1Identifier", new Fields(Constants.F_SENTIMENT.getName()));

        builder.setBolt("v2Counter", new SentimentVulgarCounterBolt(), 1)
                .fieldsGrouping("v1Identifier", new Fields(Constants.F_SENTIMENT.getName(), Constants.F_VULGARITY.getName()));

        final RabbitPublisherBolt bVulgarSentimentPublisher = new RabbitPublisherBolt(Constants.Q_SENTIMENT_VULGARITY.getName());
        builder.setBolt("v3Publisher", bVulgarSentimentPublisher, 1)
                .shuffleGrouping("v2Counter");
        return builder;
    }

    public TopologyBuilder initSentimentTopology() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TweeterSpout.class.getSimpleName(), new TweeterSpout(), 1);
        builder.setBolt("s1Identifier", new SentimentBolt(), 1)
                .shuffleGrouping(TweeterSpout.class.getSimpleName());

        builder.setBolt("s2Counter", new SentimentCounterBolt(), 1)
                .fieldsGrouping("s1Identifier", new Fields(Constants.F_SENTIMENT.getName()));

        final RabbitPublisherBolt bSentimentPublisher = new RabbitPublisherBolt(Constants.Q_SENTIMENT.getName());
        builder.setBolt("s3Publisher", bSentimentPublisher, 1)
                .shuffleGrouping("s2Counter");
        return builder;
    }

    public TopologyBuilder createTweetTopology() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TweeterSpout.class.getSimpleName(), new TweeterSpout(), 1);
        builder.setBolt("0-record-builder",
                new RecordBuilderBolt(), 1)
                .shuffleGrouping(TweeterSpout.class.getSimpleName());

        builder.setBolt("1-sentiment-income",
                new ScenarioIdentifierBolt(Constants.DB_INCOME_WKLY.getName(), "sentiment-income"), 1)
                .setNumTasks(2)
                .shuffleGrouping("0-record-builder");

        builder.setBolt("2-sentiment-green-places",
                new ScenarioIdentifierBolt(Constants.DB_GREEN_PLACES.getName(), "sentiment-green-places"), 1)
                .setNumTasks(2)
                .shuffleGrouping("1-sentiment-income");

        builder.setBolt("3-sentiment-crime",
                new ScenarioIdentifierBolt(Constants.DB_CRIME_DATA.getName(), "sentiment-vulgar-crime"), 1)
                .setNumTasks(2)
                .shuffleGrouping("2-sentiment-green-places");

        builder.setBolt("4-publisher",
                new RabbitPublisherBolt(Constants.Q_TWEETS.getName()), 1)
                .setNumTasks(2)
                .shuffleGrouping("3-sentiment-crime");
        return builder;
    }

    public Config stormConfig(int workers) {
        final Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxTaskParallelism(100);
        conf.setNumWorkers(workers);
        return conf;
    }
}
