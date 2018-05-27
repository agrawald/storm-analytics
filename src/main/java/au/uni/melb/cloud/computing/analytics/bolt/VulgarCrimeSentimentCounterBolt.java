package au.uni.melb.cloud.computing.analytics.bolt;

import au.uni.melb.cloud.computing.analytics.domain.VulgarCrimeSentiment;
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

public class VulgarCrimeSentimentCounterBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(VulgarCrimeSentimentCounterBolt.class);
    private static final Map<String, VulgarCrimeSentiment> CRIME_ENTIMENT_MAP = new HashMap<>();
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
        final String vulgarity = tuple.getStringByField(Constants.F_VULGARITY.getName());
        final String regionId = tuple.getStringByField(Constants.F_REGION_ID.getName());
        final String key = sentiment + ":" + vulgarity + ":" + region;
        VulgarCrimeSentiment vulgarCrimeSentiment;
        if (CRIME_ENTIMENT_MAP.containsKey(key)) {
            vulgarCrimeSentiment = CRIME_ENTIMENT_MAP.get(key);
        } else {
            vulgarCrimeSentiment = new VulgarCrimeSentiment(sentiment, vulgarity, regionId, region);
        }
        vulgarCrimeSentiment.increment();
        CRIME_ENTIMENT_MAP.put(key, vulgarCrimeSentiment);
        outputCollector.emit(new Values(vulgarCrimeSentiment));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.F_SENTIMENT_CRIME_COUNT.getName()));
    }
}
