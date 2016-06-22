package resa.drs;

import org.apache.storm.Config;
import org.apache.storm.scheduler.ExecutorDetails;
import resa.metrics.MeasuredData;
import resa.optimize.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.topology.ContainerContext;
import resa.util.ConfigUtil;
import resa.util.ResaUtils;

import java.util.*;
import java.util.stream.Collectors;

import static resa.util.ResaConfig.*;

/**
 * Created by ding on 14-4-26.
 */
public class ResourceScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceScheduler.class);

    private final Timer timer = new Timer(true);
    private Map<String, Integer> currAllocation;
    private int maxExecutorsPerWorker;
    private int topologyMaxExecutors;
    private Map<String, Object> conf;
    private AllocCalculator allocCalculator;
    private DecisionMaker decisionMaker;
    private ContainerContext ctx;
    private volatile List<MeasuredData> measuredDataBuffer = new ArrayList<>();

    public void init(ContainerContext containerContext) {
        this.conf = containerContext.getConfig();
        this.ctx = containerContext;
        this.ctx.addListener(new ContainerContext.Listener() {
            @Override
            public void measuredDataReceived(MeasuredData measuredData) {
                measuredDataBuffer.add(measuredData);
            }
        });
        maxExecutorsPerWorker = ConfigUtil.getInt(conf, MAX_EXECUTORS_PER_WORKER, 8);
        topologyMaxExecutors = ConfigUtil.getInt(conf, ALLOWED_EXECUTOR_NUM, -1);
        // create Allocation Calculator
        allocCalculator = ResaUtils.newInstanceThrow((String) conf.getOrDefault(ALLOC_CALC_CLASS,
                MMKAllocCalculator.class.getName()), AllocCalculator.class);
        // current allocation should be retrieved from nimbus
        currAllocation = calcAllocation(this.ctx.runningExecutors());
        allocCalculator.init(conf, Collections.unmodifiableMap(currAllocation), this.ctx.getTopology());
        // create Decision Maker
        decisionMaker = ResaUtils.newInstanceThrow((String) conf.getOrDefault(DECISION_MAKER_CLASS,
                DefaultDecisionMaker.class.getName()), DecisionMaker.class);
        decisionMaker.init(conf, containerContext.getTopology());
        LOG.info("AllocCalculator class: {}", allocCalculator.getClass().getName());
        LOG.info("DecisionMaker class: {}", decisionMaker.getClass().getName());
    }

    public void start() {
        long calcInterval = ConfigUtil.getInt(conf, OPTIMIZE_INTERVAL, 30) * 1000;
        //start optimize thread
        timer.scheduleAtFixedRate(new OptimizeTask(), calcInterval * 2, calcInterval);
        LOG.info("Init Topology Optimizer successfully with calc interval is {} ms", calcInterval);
    }

    public void stop() {
        timer.cancel();
    }

    private class OptimizeTask extends TimerTask {

        @Override
        public void run() {
            List<MeasuredData> data = measuredDataBuffer;
            measuredDataBuffer = new ArrayList<>();
            // get current ExecutorDetails from nimbus
            Map<String, List<ExecutorDetails>> topoExecutors = ctx.runningExecutors();
            // TODO: Executors == null means nimbus temporarily unreachable or this topology has been killed
            Map<String, Integer> allc = topoExecutors != null ? calcAllocation(topoExecutors) : null;
            if (allc != null && !allc.equals(currAllocation)) {
                LOG.info("Topology allocation changed");
                currAllocation = allc;
                // discard old MeasuredData
                allocCalculator.allocationChanged(Collections.unmodifiableMap(currAllocation));
            } else {
                AggResultCalculator calculator = new AggResultCalculator(data, topoExecutors,
                        ctx.getTopology());
                calculator.calCMVStat();
                //TODO: (added by Tom) we need to calc the maxProcessedDataSize as a configuration parameter.
                // set a return value (count) from calculator.calCMVStat()
                // if the count == maxProcessedDataSize (current is 500, say), we need to do something,
                // since otherwise, the measurement data is too obsolete
                Map<String, Integer> newAllocation = calcNewAllocation(calculator.getComp2ExecutorResults());
                if (newAllocation != null && !newAllocation.equals(currAllocation)) {
                    LOG.info("Detected topology allocation changed, request rebalance....");
                    LOG.info("Old allc is {}, new allc is {}", currAllocation, newAllocation);
                    ctx.requestRebalance(newAllocation, getNumWorkers(newAllocation));
                }
            }
        }

        private Map<String, Integer> calcNewAllocation(Map<String, AggResult[]> data) {
            int maxExecutors = topologyMaxExecutors == -1 ? Math.max(ConfigUtil.getInt(conf, Config.TOPOLOGY_WORKERS, 1),
                    getNumWorkers(currAllocation)) * maxExecutorsPerWorker : topologyMaxExecutors;
            Map<String, Integer> ret = null;
            try {
                AllocResult decision = allocCalculator.calc(data, maxExecutors);
                if (decision != null) {
                    ctx.emitMetric("drs.alloc", decision);
                    LOG.debug("emit drs metric {}", decision);
                }
                // tagged by Tom, modified by troy:
                // in decisionMaker , we need to improve this rebalance step to calc more stable and smooth
                // Idea 1) we can maintain an decision list, only when we have received continuous
                // decision with x times (x is the parameter), we will do rebalance (so that unstable oscillation
                // is removed)
                // Idea 2) we need to consider the expected gain (by consider the expected QoS gain) as a weight,
                // which should be contained in the AllocResult object.
                ret = decisionMaker.make(decision, Collections.unmodifiableMap(currAllocation));
            } catch (Throwable e) {
                LOG.warn("calc new allocation failed", e);
            }
            return ret;
        }
    }

    private static Map<String, Integer> calcAllocation(Map<String, List<ExecutorDetails>> topoExecutors) {
        return topoExecutors == null ? Collections.emptyMap() : topoExecutors.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
    }

    private int getNumWorkers(Map<String, Integer> allocation) {
        int totolNumExecutors = allocation.values().stream().mapToInt(Integer::intValue).sum();
        int numWorkers = totolNumExecutors / maxExecutorsPerWorker;
        if (totolNumExecutors % maxExecutorsPerWorker != 0) {
            numWorkers++;
        }
        return numWorkers;
    }

}
