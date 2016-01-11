package resa.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.ExecutorDetails;
import resa.metrics.MeasuredData;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by ding on 14/12/1.
 */
public abstract class ContainerContext {

    public static interface Listener {
        void measuredDataReceived(MeasuredData measuredData);
    }

    private StormTopology topology;
    private Map<String, Object> conf;
    private final Set<Listener> listeners = new CopyOnWriteArraySet<>();

    protected ContainerContext(StormTopology topology, Map<String, Object> conf) {
        this.topology = topology;
        this.conf = conf;
    }

    public abstract void emitMetric(String name, Object data);

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    protected Set<Listener> getListeners() {
        return listeners;
    }

    public Map<String, Object> getConfig() {
        return conf;
    }

    public StormTopology getTopology() {
        return topology;
    }

    public abstract Map<String, List<ExecutorDetails>> runningExecutors();

    public abstract boolean requestRebalance(Map<String, Integer> allocation, int numWorkers);

}
