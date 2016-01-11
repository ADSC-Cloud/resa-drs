package resa.optimize;

/**
 * Created by ding on 14-5-6.
 *
 * Modified by Tom Fu on 21-Dec-2015, for new DisruptQueue Implementation for Version after storm-core-0.10.0
 * Functions involving queue-related metrics in the current class will be affected:
 *   - Class QueueAggResult
 */
public class QueueAggResult implements Cloneable {

    private CntMeanVar queueArrivalRate;
    private CntMeanVar queueCapacity;
    private CntMeanVar queueLength;
    private CntMeanVar queueingTime;

    public QueueAggResult(double arrivalRate, double capacity, double queueLen, double qTime) {
        this();
        this.add(arrivalRate, capacity, queueLen, qTime);
    }

    public QueueAggResult() {
        this.queueArrivalRate = new CntMeanVar();
        this.queueCapacity = new CntMeanVar();
        this.queueLength = new CntMeanVar();
        this.queueingTime = new CntMeanVar();
    }

    public CntMeanVar getQueueArrivalRate() {
        return this.queueArrivalRate;
    }

    public CntMeanVar getQueueLength() {
        return this.queueLength;
    }

    public CntMeanVar getQueueCapacity() {
        return this.queueCapacity;
    }

    public CntMeanVar getQeueingTime() {
        return this.queueingTime;
    }

    public void add(double arrivalRate, double capacity, double queueLen, double qTime) {
        this.queueArrivalRate.addOneNumber(arrivalRate);
        this.queueCapacity.addOneNumber(capacity);
        this.queueLength.addOneNumber(queueLen);
        this.queueingTime.addOneNumber(qTime);
    }

    public void add(QueueAggResult r) {
        this.queueArrivalRate.addCMV(r.queueArrivalRate);
        this.queueCapacity.addCMV(r.queueCapacity);
        this.queueLength.addCMV(r.queueLength);
        this.queueingTime.addCMV(r.queueingTime);
    }

    @Override
    public String toString() {
        return String.format("avgRate: %.5f, avgQueueLen: %.5f, avgCapacity: %.5f, avgQueueingTime: %.5f", this.queueArrivalRate.getAvg(),
                this.queueLength.getAvg(), this.queueCapacity.getAvg(), this.queueingTime.getAvg());
    }
}
