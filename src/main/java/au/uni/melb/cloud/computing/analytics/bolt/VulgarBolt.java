package au.uni.melb.cloud.computing.analytics.bolt;

import au.uni.melb.cloud.computing.analytics.utils.Constants;
import com.jayway.jsonpath.JsonPath;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;

public class VulgarBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(VulgarBolt.class);

    private static final Function<String, String> GET_VULGARITY = data -> {
        try {
            return JsonPath.parse(data).read("$.vulgar", String.class);
        } catch (Exception e) {
            LOG.error("Error while fetching json path", e);
            return null;
        }
    };
    protected OutputCollector outputCollector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.info("Tuple: {}", tuple.toString());
        final String data = tuple.getStringByField(Constants.F_DATA.getName());
        final String sentiment = tuple.getStringByField(Constants.F_SENTIMENT.getName());
        final String vulgarity = GET_VULGARITY.apply(data);
        if (vulgarity != null) {
            this.outputCollector.emit(new Values(data, sentiment, vulgarity));
        }
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.F_DATA.getName(), Constants.F_SENTIMENT.getName(), Constants.F_VULGARITY.getName()));
    }
}

