package au.uni.melb.cloud.computing.analytics.bolt;

import au.uni.melb.cloud.computing.analytics.domain.ScenarioSentiment;
import au.uni.melb.cloud.computing.analytics.utils.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class GreenPlacesSentimentCounterBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GreenPlacesSentimentCounterBolt.class);
    private static final Map<String, ScenarioSentiment> GREEN_PLACES_VISIT_SENTIMENT_MAP = new HashMap<>();
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.info("Tuple: {}", tuple.toString());
        final String sentiment = tuple.getStringByField(Constants.F_SENTIMENT.getName());
        final String region = tuple.getStringByField(Constants.F_REGION.getName());
        final String regionId = tuple.getStringByField(Constants.F_REGION_ID.getName());
        final String key = sentiment + ":" + region;
        ScenarioSentiment greenPlacesVisitSentiment;
        if (GREEN_PLACES_VISIT_SENTIMENT_MAP.containsKey(key)) {
            greenPlacesVisitSentiment = GREEN_PLACES_VISIT_SENTIMENT_MAP.get(key);
        } else {
            greenPlacesVisitSentiment = new ScenarioSentiment(sentiment, "green-places-visit", region, regionId);
        }
        greenPlacesVisitSentiment.increment();
        GREEN_PLACES_VISIT_SENTIMENT_MAP.put(key, greenPlacesVisitSentiment);
        outputCollector.emit(new Values(greenPlacesVisitSentiment));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.F_SENTIMENT_GREEN_PLACES_VISIT_COUNT.getName()));
    }
}
