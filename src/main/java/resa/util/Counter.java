package resa.util;

/**
 * A simple counter.
 * <p>
 *
 * @author Troy Ding
 */
public class Counter {

    private long count;

    public Counter() {
        this(0);
    }

    public Counter(long count) {
        this.count = count;
    }

    public long incAndGet() {
        return ++count;
    }

    public long incAndGet(long v) {
        count += v;
        return count;
    }

    public long getAndInc() {
        return count++;
    }

    public long get() {
        return count;
    }
}


