package au.uni.melb.cloud.computing.analytics.bolt;

import au.uni.melb.cloud.computing.analytics.domain.VulgarSentiment;
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

public class SentimentVulgarCounterBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SentimentVulgarCounterBolt.class);
    private static final Map<String, VulgarSentiment> SENTIMENT_VULGAR_MAP = new HashMap<>();
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        final String sentiment = tuple.getStringByField(Constants.F_SENTIMENT.getName());
        final String vulgarity = tuple.getStringByField(Constants.F_VULGARITY.getName());
        final String key = sentiment + ":" + vulgarity;
        LOG.info("Tuple: {}", tuple.toString());
        final VulgarSentiment vulgarSentiment = SENTIMENT_VULGAR_MAP.containsKey(key)
                ? SENTIMENT_VULGAR_MAP.get(key)
                : new VulgarSentiment(sentiment, vulgarity);
        vulgarSentiment.increment();
        SENTIMENT_VULGAR_MAP.put(key, vulgarSentiment);
        outputCollector.emit(new Values(vulgarSentiment));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.F_SENTIMENT_VULGARITY_COUNT.getName()));
    }
}

