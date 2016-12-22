package resa.topology;

import org.apache.storm.generated.ComponentObject;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.nimbus.ITopologyValidator;
import org.apache.storm.utils.Utils;
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
 *
 * TODO: there will be an exception on registering metrics twice, when running "ResaSimExpServLoop.java" (using
 * TODO: ResaTopologyBuilder) with configuring the ResaTopologyValidator, therefore, we need to modify
 * TODO: ResaTopologyValidator to add some check to avoid register metrics twice.
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
