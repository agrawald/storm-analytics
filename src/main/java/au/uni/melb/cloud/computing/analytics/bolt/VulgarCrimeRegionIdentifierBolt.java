package au.uni.melb.cloud.computing.analytics.bolt;

import au.uni.melb.cloud.computing.analytics.domain.Feature;
import au.uni.melb.cloud.computing.analytics.domain.Properties;
import au.uni.melb.cloud.computing.analytics.utils.Constants;
import com.google.gson.JsonObject;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class VulgarCrimeRegionIdentifierBolt extends RegionIdentifierBolt {
    private static final Logger LOG = LoggerFactory.getLogger(VulgarCrimeRegionIdentifierBolt.class);

    public VulgarCrimeRegionIdentifierBolt(String dbName) {
        super(dbName);
    }

    @Override
    protected void emitTuple(final Tuple oldTuple,  final Feature feature) {
        final List<Object> values = oldTuple.getValues();
        final Properties properties = feature.getProperties();
        values.add(properties.getFeatureCode());
        values.add(properties.getFeatureName());
        outputCollector.emit(new Values(values));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.F_DATA.getName(),
                Constants.F_SENTIMENT.getName(),
                Constants.F_VULGARITY.getName(),
                Constants.F_REGION_ID.getName(),
                Constants.F_REGION.getName()));
    }
}

