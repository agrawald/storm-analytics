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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ScenarioIdentifierBolt extends RegionIdentifierBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ScenarioIdentifierBolt.class);
    private JsonParser jsonParser;
    private String tag;

    public ScenarioIdentifierBolt(final String dbName, final String tag) {
        super(dbName);
        this.tag = tag;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        jsonParser = new JsonParser();
    }

    @Override
    public void execute(Tuple tuple) {
        final Tweet record = (Tweet) tuple.getValueByField(Constants.F_RECORD.getName());
        final String data = tuple.getStringByField(Constants.F_DATA.getName());
        try {
            // tuple will have data, sentiment
            final DocumentContext dataCtx = JsonPath.parse(data);
            double[] coordinates = getCoordinates(dataCtx);
            checkInside(coordinates).ifPresent(jsonObject -> {
                record.add(this.tag);
            });
        } catch (Exception e) {
            LOG.error("Coordinates are not found");
        } finally {
            outputCollector.emit(new Values(record, data));
            outputCollector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.F_RECORD.getName(), Constants.F_DATA.getName()));
    }
}

