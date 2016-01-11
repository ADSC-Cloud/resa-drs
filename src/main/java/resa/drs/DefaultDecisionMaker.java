package resa.drs;

import resa.optimize.AllocResult;

import java.util.Map;

/**
 * Created by ding on 14-6-20.
 */
public class DefaultDecisionMaker implements DecisionMaker {

    @Override
    public Map<String, Integer> make(AllocResult newAllocResult, Map<String, Integer> currAlloc) {
        return newAllocResult.currOptAllocation;
    }
}
