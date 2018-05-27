package au.uni.melb.cloud.computing.analytics.domain;

public class WorkLife implements Stat {
    private long lga_code06;
    private String lga_name06;
    private double numeric;
    private double ci_low;
    private double ci_high;
    private String significance;
    private double vic_ave;
    private String feature_code;
    private String feature_name;

    public long getLga_code06() {
        return lga_code06;
    }

    public void setLga_code06(long lga_code06) {
        this.lga_code06 = lga_code06;
    }

    public String getLga_name06() {
        return lga_name06;
    }

    public void setLga_name06(String lga_name06) {
        this.lga_name06 = lga_name06;
    }

    public double getNumeric() {
        return numeric;
    }

    public void setNumeric(double numeric) {
        this.numeric = numeric;
    }

    public double getCi_low() {
        return ci_low;
    }

    public void setCi_low(double ci_low) {
        this.ci_low = ci_low;
    }

    public double getCi_high() {
        return ci_high;
    }

    public void setCi_high(double ci_high) {
        this.ci_high = ci_high;
    }

    public String getSignificance() {
        return significance;
    }

    public void setSignificance(String significance) {
        this.significance = significance;
    }

    public double getVic_ave() {
        return vic_ave;
    }

    public void setVic_ave(double vic_ave) {
        this.vic_ave = vic_ave;
    }

    public String getFeature_code() {
        return feature_code;
    }

    public void setFeature_code(String feature_code) {
        this.feature_code = feature_code;
    }

    @Override
    public String getFeatureName() {
        return feature_name;
    }

    public void setFeature_name(String feature_name) {
        this.feature_name = feature_name;
    }
}
