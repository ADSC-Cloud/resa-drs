package resa.metrics;

import java.util.Map;

/**
 * Created by ding on 14-4-28.
 */
public class MeasuredData {

    public final long timestamp;
    public final String component;
    public final int task;
    public final Map<String, Object> data;

    public MeasuredData(String component, int task, long timestamp, Map<String, Object> data) {
        this.timestamp = timestamp;
        this.component = component;
        this.task = task;
        this.data = data;
    }
}
