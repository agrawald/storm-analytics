package au.uni.melb.cloud.computing.analytics.bolt;

import au.uni.melb.cloud.computing.analytics.domain.Tweet;
import au.uni.melb.cloud.computing.analytics.utils.Constants;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jayway.jsonpath.DocumentContext;
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

public class RecordBuilderBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(RecordBuilderBolt.class);
    private OutputCollector outputCollector;
    private JsonParser jsonParser;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        jsonParser = new JsonParser();
    }

    protected double[] getCoordinates(final DocumentContext dataCtx) {
        try {
            return new double[]{
                    dataCtx.read("$.tweet_data.geo.coordinates[0]", Double.class),
                    dataCtx.read("$.tweet_data.geo.coordinates[1]", Double.class)
            };
        } catch (Exception e) {
            LOG.error("Coordinates are empty.", e);
            return null;
        }
    }

    private Tweet getRecord(final Tuple tuple, final double[] coordinates, final DocumentContext dataCtx) {
        if (tuple.contains(Constants.F_RECORD.getName())) {
            return (Tweet) tuple.getValueByField(Constants.F_RECORD.getName());
        } else {
            Tweet record = new Tweet();
            if (coordinates != null) {
                record.setGeo(coordinates);
            }
            record.setScore(dataCtx.read("$.sentiment", String.class));
            record.setVulgar(dataCtx.read("$.vulgar", String.class));
            return record;
        }
    }

    @Override
    public void execute(Tuple tuple) {
        // tuple will have data, sentiment
        LOG.info("Tuple: {}", tuple.toString());
        final String data = tuple.getStringByField(Constants.F_DATA.getName());
        final DocumentContext dataCtx = JsonPath.parse(data);
        final Tweet record = getRecord(tuple, getCoordinates(dataCtx), dataCtx);
        outputCollector.emit(new Values(record, data));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.F_RECORD.getName(),
                Constants.F_DATA.getName()));
    }
}

