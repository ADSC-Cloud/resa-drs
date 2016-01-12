package resa.topology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * Created by ding on 14-6-9.
 */
public class DelegatedSpout implements IRichSpout {

    private IRichSpout delegate;
    private byte[] serializedSpout;

    public DelegatedSpout() {
    }

    public DelegatedSpout(IRichSpout delegate) {
        setSpout(delegate);
    }

    public DelegatedSpout(byte[] serializedSpout) {
        setSerializedSpout(serializedSpout);
    }

    public void setSpout(IRichSpout delegate) {
        this.delegate = delegate;
        this.serializedSpout = null;
    }

    public void setSerializedSpout(byte[] data) {
        this.delegate = null;
        this.serializedSpout = data;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return delegate.getComponentConfiguration();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        if (delegate == null) {
            delegate = Utils.javaDeserialize(serializedSpout, IRichSpout.class);
            serializedSpout = null;
        }
        delegate.open(conf, context, collector);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void activate() {
        delegate.activate();
    }

    @Override
    public void deactivate() {
        delegate.deactivate();
    }

    @Override
    public void nextTuple() {
        delegate.nextTuple();
    }

    @Override
    public void ack(Object msgId) {
        delegate.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        delegate.fail(msgId);
    }

}
