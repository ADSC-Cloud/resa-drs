package TestTopology.simulated;

import TestTopology.helper.RedisQueueSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

public class TASentenceSpout extends RedisQueueSpout {

    private transient long count = 0;
    private String spoutIdPrefix = "s-";

    public TASentenceSpout(String host, int port, String queue) {
        super(host, port, queue);
    }

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        super.open(map, context, collector);
        spoutIdPrefix = spoutIdPrefix + context.getThisTaskId() + '-';
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sid", "sentence"));
    }

    @Override
    protected void emitData(Object data) {
        String id = spoutIdPrefix + count;
        count++;
        collector.emit(new Values(id, data), id);
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {

    }
}