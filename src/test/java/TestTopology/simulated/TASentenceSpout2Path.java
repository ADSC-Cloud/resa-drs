package TestTopology.simulated;

import TestTopology.helper.RedisQueueSpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class TASentenceSpout2Path extends RedisQueueSpout {

    private transient long count = 0;
    private String spoutIdPrefix = "s-2Path";
    private transient Random rand;
    private double p;

    public TASentenceSpout2Path(String host, int port, String queue, double p) {
        super(host, port, queue);
        this.p = p;
    }

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        super.open(map, context, collector);
        spoutIdPrefix = spoutIdPrefix + context.getThisTaskId() + '-';
        rand = new Random();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("P-Stream", new Fields("sid", "sentence"));
        declarer.declareStream("NotP-Stream", new Fields("sid", "sentence"));
    }

    @Override
    protected void emitData(Object data) {
        String id = spoutIdPrefix + count;
        count++;        
        
        double prob = rand.nextDouble();
        if (prob < this.p){
        	collector.emit("P-Stream", new Values(id, data), id);
        }
        else{
        	collector.emit("NotP-Stream", new Values(id, data), id);
        }
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }
}