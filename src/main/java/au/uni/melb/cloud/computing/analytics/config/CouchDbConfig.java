package au.uni.melb.cloud.computing.analytics.config;

import au.uni.melb.cloud.computing.analytics.domain.Feature;
import com.google.gson.JsonObject;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public enum CouchDbConfig {
    INSTANCE;
    private Properties properties = new Properties();
    private Map<String, List<Feature>> scenarioFeatures = new HashMap<>();

    CouchDbConfig() {
        try {
            final InputStream is = CouchDbConfig.class.getClassLoader().getResourceAsStream("couchdb.properties");
            properties.load(is);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<Feature> getFeatures(String dbName) {
        if (scenarioFeatures.containsKey(dbName)) {
            return scenarioFeatures.get(dbName);
        }
        final CouchDbClient client = INSTANCE.client(dbName);
        final List<Feature> features = client.view("_all_docs")
                .includeDocs(true)
                .query(Feature.class);
        scenarioFeatures.put(dbName, features);
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return features;
    }

    private CouchDbProperties couchDbProperties(final String dbName) {
        return new CouchDbProperties()
                .setProtocol(properties.getProperty("couchdb.protocol"))
                .setHost(properties.getProperty("couchdb.host"))
                .setPort(Integer.parseInt(properties.getProperty("couchdb.port")))
                .setDbName(dbName);
    }

    public CouchDbClient client(final String dbName) {
        return new CouchDbClient(couchDbProperties(dbName));
    }
}
