package au.uni.melb.cloud.computing.analytics.bolt;

import au.uni.melb.cloud.computing.analytics.domain.Sentiment;
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

public class SentimentCounterBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SentimentCounterBolt.class);
    private static final Map<String, Sentiment> SENTIMENT_MAP = new HashMap<>();
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        final String key = tuple.getStringByField(Constants.F_SENTIMENT.getName());
        LOG.info("Tuple: {}", tuple.toString());
        final Sentiment sentiment = SENTIMENT_MAP.containsKey(key)
                ? SENTIMENT_MAP.get(key)
                : new Sentiment(key);
        sentiment.increment();
        SENTIMENT_MAP.put(key, sentiment);
        outputCollector.emit(new Values(sentiment));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.F_SENTIMENT_COUNT.getName()));
    }
}
