package resa.optimize;

import org.apache.storm.generated.StormTopology;
import resa.util.FixedSizeQueue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * Created by ding on 14-4-30.
 * Modified by Tom Fu on 21-Dec-2015, for new DisruptQueue Implementation for Version after storm-core-0.10.0
 * Functions and Classes involving queue-related metrics in the current class will be affected:
 */
class HistoricalCollectedData {

    public HistoricalCollectedData(StormTopology rawTopology, int historySize) {
        this.rawTopology = rawTopology;
        this.historySize = historySize;
    }

    private StormTopology rawTopology;
    private int historySize;
    public final Map<String, Queue<AggResult>> compHistoryResults = new HashMap<>();

    public void putResult(String comp, AggResult[] exeAggResult) {
        AggResult CompAggResult = rawTopology.get_spouts().containsKey(comp) ? new SpoutAggResult() : new BoltAggResult();
        ///First aggregate executorResults to Component results, vertical combine.
        AggResult.getVerticalCombinedResult(CompAggResult, Arrays.asList(exeAggResult));
        ///Next, add aggregated component results to history window
        compHistoryResults.computeIfAbsent(comp, (k) -> new FixedSizeQueue(historySize)).add(CompAggResult);
    }

    public void clear() {
        compHistoryResults.clear();
    }

}
