package resa.topology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.Map;

/**
 * Created by ding on 14-6-9.
 */
public class DelegatedSpout implements IRichSpout {

    private IRichSpout delegate;

    public DelegatedSpout(IRichSpout delegate) {
        this.delegate = delegate;
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
