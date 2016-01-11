package resa.drs;

import resa.optimize.AllocResult;

import java.util.Map;

/**
 * Created by ding on 15/2/5.
 */
public class EmptyDecisionMaker implements DecisionMaker {
    @Override
    public Map<String, Integer> make(AllocResult newAllocResult, Map<String, Integer> currAlloc) {
        return null;
    }
}
