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

public class IncomeWklySentimentCounterBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(IncomeWklySentimentCounterBolt.class);
    private static final Map<String, ScenarioSentiment> INCOME_SENTIMENT_MAP = new HashMap<>();
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
        ScenarioSentiment scenarioSentiment;
        if (INCOME_SENTIMENT_MAP.containsKey(key)) {
            scenarioSentiment = INCOME_SENTIMENT_MAP.get(key);
        } else {
            scenarioSentiment = new ScenarioSentiment(sentiment, "sentiment-income", region, regionId);
        }
        scenarioSentiment.increment();
        INCOME_SENTIMENT_MAP.put(key, scenarioSentiment);
        outputCollector.emit(new Values(scenarioSentiment));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.F_SENTIMENT_INCOME_WKLY_COUNT.getName()));
    }
}
