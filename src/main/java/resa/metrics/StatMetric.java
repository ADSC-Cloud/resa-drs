package resa.metrics;

import backtype.storm.metric.api.IMetric;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-8-12.
 */
public class StatMetric implements IMetric {

    private double[] xAxis;
    private Map<String, long[]> data;

    public StatMetric(double[] xAxis) {
        this.xAxis = xAxis;
        if (xAxis != null && xAxis.length > 0) {
            data = new HashMap<>();
        }
    }

    public void add(String key, double value) {
        int pos = Arrays.binarySearch(xAxis, value);
        if (pos < 0) {
            pos = -pos - 1;
        }
        data.computeIfAbsent(key, k -> new long[xAxis.length + 1])[pos]++;
//        long[] stat = data.computeIfAbsent(key, k -> new long[(xAxis.length + 1) * 2]);
//        stat[pos * 2]++;
//        stat[pos * 2 + 1] = Double.doubleToLongBits(Double.longBitsToDouble(stat[pos * 2 + 1]) + value);
    }

    @Override
    public Object getValueAndReset() {
        if (data == null || data.isEmpty()) {
            return null;
        }
        Map<String, String> ret = new HashMap<>();
        data.forEach((k, v) -> ret.put(k, stat2String(v)));
        data = new HashMap<>();
        return ret;
    }

    private String stat2String(long[] statData) {
        StringBuilder sb = new StringBuilder();
        sb.append(statData[0]);
        for (int i = 1; i < statData.length; i++) {
            sb.append(',');
            sb.append(statData[i]);
        }
//        for (int i = 2; i < statData.length; i += 2) {
//            sb.append(',');
//            sb.append(statData[i]);
//        }
//        sb.append(";");
//        sb.append(Double.longBitsToDouble(statData[1]));
//        for (int i = 3; i < statData.length; i += 2) {
//            sb.append(',');
//            sb.append(Double.longBitsToDouble(statData[i]));
//        }
        return sb.toString();
    }

}
