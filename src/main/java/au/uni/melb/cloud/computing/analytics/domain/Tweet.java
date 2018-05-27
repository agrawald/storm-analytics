package au.uni.melb.cloud.computing.analytics.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Tweet implements Serializable {
    private double[] geo;
    private String score;
    private String vulgar;
    private List<String> scenarios = new ArrayList<>();

    public double[] getGeo() {
        return geo;
    }

    public void setGeo(double[] geo) {
        this.geo = geo;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }

    public String getVulgar() {
        return vulgar;
    }

    public void setVulgar(String vulgar) {
        this.vulgar = vulgar;
    }

    public List<String> getScenarios() {
        return scenarios;
    }

    public void setScenarios(List<String> scenarios) {
        this.scenarios = scenarios;
    }

    public void add(String scenario) {
        this.scenarios.add(scenario);
    }
}
