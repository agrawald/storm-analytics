package au.uni.melb.cloud.computing.analytics.domain;

import java.io.Serializable;

public class Feature implements Serializable {
    private double[][] coordinates;
    private Properties properties;

    public double[][] getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(double[][] coordinates) {
        this.coordinates = coordinates;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
