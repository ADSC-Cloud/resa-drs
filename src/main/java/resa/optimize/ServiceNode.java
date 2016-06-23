package resa.optimize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tom.fu on 22/6/2016.
 * Modified by Tom Fu on 21-Dec-2015, for new DisruptQueue Implementation for Version after storm-core-0.10.0
 * Functions involving queue-related metrics in the current class will be affected:
 */
public class ServiceNode {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceNode.class);

    protected String componentID;
    protected int executorNumber;
    protected double compSampleRate;

    protected double avgSendQueueLength;
    protected double avgRecvQueueLength;

    protected double avgServTimeHis;
    protected double scvServTimeHis;

    protected double numCompleteTuples;
    protected double sumDurationSeconds;
    protected double tupleCompleteRate;

    /*metrics on recv_queue*/
    protected double lambda;
    protected double mu;
    protected double rho;

    /* second order information of arrivals */
    protected double interArrivalScv;

    protected double exArrivalRate;
    protected double ratio;


    public ServiceNode(String componentID, int executorNumber, double compSampleRate,
                       BoltAggResult ar, double exArrivalRate){
        this.componentID = componentID;
        this.executorNumber = executorNumber;
        this.compSampleRate = compSampleRate;

        this.avgSendQueueLength = ar.getAvgSendQueueLength();
        this.avgRecvQueueLength = ar.getAvgRecvQueueLength();

        this.avgServTimeHis = ar.getAvgServTimeHis();
        this.scvServTimeHis = ar.getScvServTimeHis();

        this.numCompleteTuples = ar.getNumCompleteTuples();
        this.sumDurationSeconds = ar.getDurationSeconds();
        this.tupleCompleteRate = numCompleteTuples * executorNumber / (sumDurationSeconds * compSampleRate);

        this.lambda = ar.getArrivalRatePerSec() * executorNumber;
        this.interArrivalScv = ar.getInterArrivalTimeScv();

        this.exArrivalRate = exArrivalRate;
        this.mu = this.avgServTimeHis > 0.0 ? (1000.0 / this.avgServTimeHis) : Double.MAX_VALUE;
        this.rho = lambda * avgServTimeHis / (executorNumber * 1000.0);

        this.ratio = this.exArrivalRate > 0.0 ? (this.lambda / this.exArrivalRate) : 0;

        LOG.info("ServiceNode is created: " + toString());
    }


    public String getComponentID() {
        return componentID;
    }

    public int getExecutorNumber() {
        return executorNumber;
    }

    public double getCompSampleRate() {
        return compSampleRate;
    }

    public double getAvgSendQueueLength() {
        return avgSendQueueLength;
    }

    public double getAvgRecvQueueLength() {
        return avgRecvQueueLength;
    }

    public double getAvgServTimeHis() {
        return avgServTimeHis;
    }

    public double getScvServTimeHis() {
        return scvServTimeHis;
    }

    public double getNumCompleteTuples() {
        return numCompleteTuples;
    }

    public double getSumDurationSeconds() {
        return sumDurationSeconds;
    }

    public double getTupleCompleteRate() {
        return tupleCompleteRate;
    }

    public double getLambda() {
        return lambda;
    }

    public double getInterArrivalScv() {
        return interArrivalScv;
    }

    public double getExArrivalRate() {
        return exArrivalRate;
    }

    public double getMu() {
        return mu;
    }

    public double getRatio() {
        return ratio;
    }

    public double getRho() {
        return rho;
    }


    @Override
    public String toString() {
        return String.format(
                "(ID, eNum):(%s,%d), ProcRate: %.3f, avgSTime: %.3f, scvSTime: %.3f, mu: %.3f, ProcCnt: %.1f, Dur: %.1f, sample: %.1f, SQLen: %.1f, RQLen: %.1f, " +
                        "-----> arrRateAvg: %.3f, arrRateScv: %.3f, ratio: %.3f, rho: %.3f",
                componentID, executorNumber, tupleCompleteRate, avgServTimeHis, scvServTimeHis, mu,
                numCompleteTuples, sumDurationSeconds, compSampleRate, avgSendQueueLength, avgRecvQueueLength,
                lambda, interArrivalScv, ratio, rho);
    }
}
