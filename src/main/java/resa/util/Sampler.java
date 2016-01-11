package resa.util;

/**
 * Created by ding on 14-4-26.
 */
public class Sampler {

    private int sampleValue;
    private long counter = 0;

    public Sampler(double rate) {
        if (Double.compare(0, rate) > 0 || Double.compare(rate, 1) > 0) {
            throw new IllegalArgumentException("Bad sample rate: " + rate);
        }
        sampleValue = (int) (1.0 / rate);
    }

    public boolean shoudSample() {
        return counter++ % sampleValue == 0;
    }

}
