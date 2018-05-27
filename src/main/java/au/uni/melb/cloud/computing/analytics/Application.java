package au.uni.melb.cloud.computing.analytics;

import org.apache.storm.StormSubmitter;

import static au.uni.melb.cloud.computing.analytics.config.TopologyConfig.INSTANCE;

public class Application {
    public static void main(String[] args) throws Exception {
        StormSubmitter.submitTopologyWithProgressBar("sentiment-crime-topology",
                INSTANCE.stormConfig(1),
                INSTANCE.initCrimeTopology().createTopology());
        StormSubmitter.submitTopologyWithProgressBar("sentiment-topology",
                INSTANCE.stormConfig(1),
                INSTANCE.initSentimentTopology().createTopology());
        StormSubmitter.submitTopologyWithProgressBar("sentiment-vulgar-topology",
                INSTANCE.stormConfig(1),
                INSTANCE.initVulgarTopology().createTopology());
        StormSubmitter.submitTopologyWithProgressBar("sentiment-green-places-topology",
                INSTANCE.stormConfig(1),
                INSTANCE.initGreenPlacesTopology().createTopology());
        StormSubmitter.submitTopologyWithProgressBar("sentiment-income-wkly-topology",
                INSTANCE.stormConfig(1),
                INSTANCE.initIncomeWklyTopology().createTopology());
        StormSubmitter.submitTopologyWithProgressBar("tweet-topology",
                INSTANCE.stormConfig(1),
                INSTANCE.createTweetTopology().createTopology());

    }
}
