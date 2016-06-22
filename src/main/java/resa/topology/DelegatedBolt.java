package resa.topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Created by ding on 14-6-9.
 */
public class DelegatedBolt implements IRichBolt {

    private IRichBolt delegate;
    private byte[] serializedBolt;

    public DelegatedBolt() {}

    public DelegatedBolt(IRichBolt delegate) {
        setBolt(delegate);
    }

    public DelegatedBolt(byte[] serializedBolt) {
        setSerializedBolt(serializedBolt);
    }

    public void setBolt(IRichBolt delegate) {
        this.delegate = delegate;
        this.serializedBolt = null;
    }

    public void setSerializedBolt(byte[] data) {
        this.delegate = null;
        this.serializedBolt = data;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        if (delegate == null) {
            delegate = Utils.javaDeserialize(serializedBolt, IRichBolt.class);
            serializedBolt = null;
        }
        delegate.prepare(stormConf, context, collector);
    }

    @Override
    public void execute(Tuple input) {
        delegate.execute(input);
    }

    @Override
    public void cleanup() {
        delegate.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return delegate.getComponentConfiguration();
    }

    public IRichBolt getDelegate() {
        return delegate;
    }
}
