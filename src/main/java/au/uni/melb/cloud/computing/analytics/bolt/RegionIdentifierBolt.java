package au.uni.melb.cloud.computing.analytics.bolt;

import au.uni.melb.cloud.computing.analytics.config.CouchDbConfig;
import au.uni.melb.cloud.computing.analytics.domain.Feature;
import au.uni.melb.cloud.computing.analytics.domain.Properties;
import au.uni.melb.cloud.computing.analytics.utils.Constants;
import com.google.gson.Gson;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jillesvangurp.geo.GeoGeometry;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RegionIdentifierBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(RegionIdentifierBolt.class);
    protected OutputCollector outputCollector;
    private String dbName;
    private List<Feature> features;

    public RegionIdentifierBolt(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        this.features = CouchDbConfig.INSTANCE.getFeatures(dbName);
    }

    protected double[] getCoordinates(final DocumentContext dataCtx) {
        return new double[]{
                dataCtx.read("$.tweet_data.geo.coordinates[1]", Double.class),
                dataCtx.read("$.tweet_data.geo.coordinates[0]", Double.class)
        };
    }

    @Override
    public void execute(Tuple tuple) {
        // tuple will have data, sentiment
        LOG.info("Tuple: {}", tuple.toString());
        final String data = tuple.getStringByField(Constants.F_DATA.getName());
        final DocumentContext dataCtx = JsonPath.parse(data);
        try {
            double[] coordinates = getCoordinates(dataCtx);
            checkInside(coordinates).ifPresent(oFeature -> emitTuple(tuple, oFeature));
        } catch (RuntimeException rte) {
            LOG.error("Error while getting the coordinates", rte);
        }
    }

    protected void emitTuple(final Tuple oldTuple, final Feature feature) {
        final Properties properties = feature.getProperties();
        outputCollector.emit(new Values(oldTuple.getStringByField(Constants.F_DATA.getName()),
                oldTuple.getStringByField(Constants.F_SENTIMENT.getName()),
                properties.getFeatureName(),
                properties.getFeatureCode()));
    }

    protected Optional<Feature> checkInside(final double[] coordinates) {
        if (coordinates != null && coordinates.length > 0) {
            return features.parallelStream()
                    .filter(feature -> GeoGeometry.polygonContains(coordinates, feature.getCoordinates()))
                    .findFirst();

        }
        return Optional.empty();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.F_DATA.getName(),
                Constants.F_SENTIMENT.getName(),
                Constants.F_REGION.getName(),
                Constants.F_REGION_ID.getName()));
    }
}

