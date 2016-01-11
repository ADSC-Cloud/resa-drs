package resa.optimize;

/**
 * Created by Tom.fu on 16/4/2014.
 * Modified by Tom Fu on 21-Dec-2015, for new DisruptQueue Implementation for Version after storm-core-0.10.0
 * Functions involving queue-related metrics in the current class will be affected:
 *  - we do not use duration any more.
 */
public class AggResult implements Cloneable {

    /**
     * The measured duration is in milliseconds
     */
    protected long duration = 0;
    protected QueueAggResult sendQueueResult = new QueueAggResult();
    protected QueueAggResult recvQueueResult = new QueueAggResult();

    /**
     * Vertical Combine is used for combine executor-level results to component-level results
     * At this implementation, vertical combine and horizontal combine are the same.
     * @param dest
     * @param data
     * @param <T>
     * @return
     */
    public static <T extends AggResult> T getVerticalCombinedResult(T dest, Iterable<AggResult> data) {
        data.forEach(dest::add);
        return dest;
    }

    /**
     * Horizontal Combine is used for combine results stored in the historical window, window-based combination
     * At this implementation, vertical combine and horizontal combine are the same.
     * @param dest
     * @param data
     * @param <T>
     * @return
     */
    public static <T extends AggResult> T getHorizontalCombinedResult(T dest, Iterable<AggResult> data) {
        data.forEach(dest::add);
        return dest;
    }

    ///When we use CMV model for the QueueAggResult, for an executor with multiple tasks,
    ///Then, we require only one of the tasks is counted (by taking first tasks method),
    ///otherwise, when we call CMV.getAvg, this is a weighted average by number of values inserted,
    ///An executor with more number of tasks will insert move values (they are actually almost same)
    ///On the other hand, TODO, if we allow duplicate report value by tasks belonging to the same executor,
    ///Here when we combine the results, we can take some reduce methods (like mean reducer), to deal with these values.
    public void add(AggResult r) {
        this.sendQueueResult.add(r.sendQueueResult);
        this.recvQueueResult.add(r.recvQueueResult);
        //CMVAdd will check null or not, no need to check here.
        //Objects.requireNonNull(r, "input AggResult cannot null");
        this.duration += r.duration;
    }

    public QueueAggResult getSendQueueResult() {
        return sendQueueResult;
    }

    public QueueAggResult getRecvQueueResult() {
        return recvQueueResult;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public void addDuration(long duration) {
        this.duration += duration;
    }

    public long getDurationMilliseconds(){
        return this.duration;
    }

    /**
     * The original duration and measured result in millisecond
     * @return the measured duration in seconds
     */
    public double getDurationSeconds(){
        return this.duration / 1000.0;
    }
}
