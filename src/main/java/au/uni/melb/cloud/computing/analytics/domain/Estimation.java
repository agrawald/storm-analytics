package au.uni.melb.cloud.computing.analytics.domain;

import java.io.Serializable;

public class Estimation implements Serializable {
    private int count;

    public int getCount() {
        return count;
    }

    public void increment() {
        count++;
    }
}
