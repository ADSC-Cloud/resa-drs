package resa.drs;

import backtype.storm.generated.StormTopology;
import resa.optimize.AllocResult;

import java.util.Map;

/**
 * Created by ding on 14-6-20.
 */
public interface DecisionMaker {

    /**
     * Called when a new instance was created.
     *
     * @param conf
     * @param rawTopology
     */
    default void init(Map<String, Object> conf, StormTopology rawTopology) {
    }

    Map<String, Integer> make(AllocResult newAllocResult, Map<String, Integer> currAlloc);


}
