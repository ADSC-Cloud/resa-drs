package resa.topology;

import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.nimbus.ITopologyValidator;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.metrics.MeasurableBolt;
import resa.metrics.MeasurableSpout;

import java.util.Map;

/**
 * Developed by Tom Fu, on Jan 7th, 2016
 * The ResaTopologyValidator will replace the DefaultTopologyValidator
 * It requires to modify the storm configuration file: storm.yaml
 * Add this entry:
 * nimbus.topology.validator: "resa.topology.ResaTopologyValidator"
 */
public class ResaTopologyValidator implements ITopologyValidator {

    private static final Logger LOG = LoggerFactory.getLogger(ResaTopologyValidator.class);

    @Override
    public void prepare(Map StormConf) {
        LOG.info("Preparing ResaTopologyValidator");
    }

    @Override
    public void validate(String topologyName, Map topologyConf, StormTopology topology) throws InvalidTopologyException {

        topology.get_spouts().forEach((k, v) -> {
            MeasurableSpout s = new MeasurableSpout();
            s.setSerializedSpout(v.get_spout_object().get_serialized_java());
            v.set_spout_object(ComponentObject.serialized_java(Utils.javaSerialize(s)));
        });

        topology.get_bolts().forEach((k, v) -> {
            MeasurableBolt b = new MeasurableBolt();
            b.setSerializedBolt(v.get_bolt_object().get_serialized_java());
            v.set_bolt_object(ComponentObject.serialized_java(Utils.javaSerialize(b)));
        });
    }
}
