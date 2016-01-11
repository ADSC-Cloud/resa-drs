package resa.metrics;

import resa.util.Counter;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-9-10.
 */
class CompleteStatMetric extends StatMetric {

    private Map<String, Counter> counter = new HashMap<>();

    public CompleteStatMetric(double[] xAxis) {
        super(xAxis);
    }

    public void fail(String key) {
        counter.computeIfAbsent(key, k -> new Counter(0)).incAndGet();
    }

    @Override
    public Object getValueAndReset() {
        Map<String, String> ret = (Map<String, String>) super.getValueAndReset();
        if (ret == null) {
            return null;
        }
        ret.entrySet().forEach(e -> e.setValue(e.getValue() + "|"
                + counter.computeIfAbsent(e.getKey(), k -> new Counter(0)).get()));
        counter.clear();
        return ret;
    }
}
