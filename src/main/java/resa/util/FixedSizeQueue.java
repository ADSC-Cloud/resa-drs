package resa.util;

import java.util.LinkedList;

/**
 * Created by ding on 14-4-30.
 */
public class FixedSizeQueue extends LinkedList {

    public FixedSizeQueue(int maxSize) {
        this.maxSize = maxSize;
    }

    private int maxSize;

    @Override
    public boolean add(Object o) {
        boolean ret = super.add(o);
        if (size() > maxSize) {
            poll();
        }
        return ret;
    }
}
