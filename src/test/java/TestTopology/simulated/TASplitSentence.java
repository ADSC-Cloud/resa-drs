package TestTopology.simulated;

import TestTopology.helper.IntervalSupplier;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ding on 14-1-27.
 */
public class TASplitSentence extends TASleepBolt {
    private static final long serialVersionUID = 9182719848878455933L;
    public static final Logger LOG = LoggerFactory.getLogger(TASplitSentence.class);


    public TASplitSentence(IntervalSupplier sleep) {
        super(sleep);
    }

    public void execute(Tuple tuple) {
        super.execute(tuple);
        String sid = tuple.getString(0);
        String sentence = tuple.getString(1);
//        StringTokenizer tokenizer = new StringTokenizer(sentence);
//        while (tokenizer.hasMoreTokens()) {
//            collector.emit(tuple, new Values(sid, tokenizer.nextToken()));
//        }
        if (sid.hashCode() % 10000 == 0) {
            LOG.info(sid + "," + sentence);
        }
        collector.emit(tuple, new Values(sid, sentence));
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sid", "sentence"));
    }
}