package au.uni.melb.cloud.computing.analytics.domain;

import java.io.Serializable;

public class Properties implements Serializable {
    private String feature_code;
    private String feature_name;

    public String getFeatureCode() {
        return feature_code;
    }

    public void setFeature_code(String feature_code) {
        this.feature_code = feature_code;
    }

    public String getFeatureName() {
        return feature_name;
    }

    public void setFeature_name(String feature_name) {
        this.feature_name = feature_name;
    }
}
