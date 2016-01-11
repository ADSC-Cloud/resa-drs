package resa.optimize;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-5-6.
 */
public class BoltAggResult extends AggResult {

    private Map<String, CntMeanVar> tupleProcess = new HashMap<>();

    public Map<String, CntMeanVar> getTupleProcess() {
        return tupleProcess;
    }

    public CntMeanVar getCombinedProcessedResult() {
        CntMeanVar retVal = new CntMeanVar();
        tupleProcess.values().stream().forEach(retVal::addCMV);
        return retVal;
    }

    @Override
    public void add(AggResult r) {
        super.add(r);
        ((BoltAggResult) r).tupleProcess.forEach((s, cntMeanVar) ->
                this.tupleProcess.computeIfAbsent(s, (k) -> new CntMeanVar()).addCMV(cntMeanVar));
    }
}
