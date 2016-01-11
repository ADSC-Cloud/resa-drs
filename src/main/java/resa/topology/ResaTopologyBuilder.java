package resa.topology;

import backtype.storm.topology.*;
import resa.metrics.MeasurableBolt;
import resa.metrics.MeasurableSpout;

/**
 * Created by ding on 14-4-26.
 */
public class ResaTopologyBuilder extends TopologyBuilder {

    @Override
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) {
        bolt = new MeasurableBolt(bolt);
        return super.setBolt(id, bolt, parallelismHint);
    }

    @Override
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelismHint) {
        spout = new MeasurableSpout(spout);
        return super.setSpout(id, spout, parallelismHint);
    }
}
