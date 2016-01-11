package TestTopology.simulated;

import TestTopology.helper.IntervalSupplier;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Random;

/**
 * Created by ding on 14-1-27.
 */
public class TAWordCounter2Path extends TASleepBolt {

    private Random rand;
    private double p;

    public TAWordCounter2Path(IntervalSupplier sleep, double p) {
        super(sleep);
        this.p = p;
        rand = new Random();
    }

    @Override
    public void execute(Tuple tuple) {

        super.execute(tuple);
        String sid = tuple.getString(0);
        String word = tuple.getString(1);        
        
        double prob = rand.nextDouble();
        if (prob < this.p){
        	collector.emit("P-Stream", tuple, new Values(sid, word + "!"));
        }
        else{
        	collector.emit("NotP-Stream", new Values(word));
        }        
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("P-Stream", new Fields("sid", "word"));
        declarer.declareStream("NotP-Stream", new Fields("word-finished"));
    }
}
