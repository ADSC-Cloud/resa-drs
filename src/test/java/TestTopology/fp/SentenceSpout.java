package TestTopology.fp;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import TestTopology.helper.RedisQueueSpout;


/**
 * Created by ding on 14-6-5.
 */
public class SentenceSpout extends RedisQueueSpout implements Constant {

    public SentenceSpout(String host, int port, String queue) {
        super(host, port, queue);
    }

    @Override
    protected void emitData(Object data) {
        String text = (String) data;
        collector.emit(new Values(((String) data).substring(1), text.charAt(0) == '+'), "");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SENTENCE_FIELD, IS_ADD_FIELD));
    }
}
