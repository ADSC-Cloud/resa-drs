package resa.optimize;

import java.util.Objects;

/**
 * Created by Tom.fu on 16/4/2014.
 */
public class CntMeanVar {
    private long count = 0;
    private double sum = 0;
    private double squareSum = 0;

    ///Add one measured valued
    void addOneNumber(double num) {
        count++;
        sum += num;
        squareSum += (num * num);
    }

    ///Add aggregated measure values in one window
    void addAggWin(int aggCount, double aggSum, double aggSquareSum) {
        count += aggCount;
        sum += aggSum;
        squareSum += aggSquareSum;
    }

    void addCMV(CntMeanVar cmv) {
        ///cmv should not be null here
        Objects.requireNonNull(cmv);
        count += cmv.count;
        sum += cmv.sum;
        squareSum += cmv.squareSum;
    }

    void clear() {
        count = 0;
        sum = 0;
        squareSum = 0;
    }

    long getCount() {
        return count;
    }

    double getSum() {
        return sum;
    }

    double getAvg() {
        return count == 0 ? 0.0 : sum / (double) count;
    }

    double getAvg2() {
        return count == 0 ? 0.0 : squareSum / (double) count;
    }

    double getVar() {
        double avg = getAvg();
        return getAvg2() - avg * avg;
    }

    /**
     * Var(X) = E(X*X) - E(X)*E(X)
     * Square coefficient of variation (Scv), scv(X) = Var(X) / [E(X)*E(X)];
     * @return scv(X) = Var(X) / [E(X)*E(X)] = E(X*X) / [E(X)*E(X)] - 1
     *
     */
    double getScv() {
        return count == 0 ? 0.0 : (getAvg2() / (getAvg() * getAvg()) - 1.0);
    }

    String toCMVString() {
        return "Count: " + getCount()
                + String.format(", sum: %.2f", getSum())
                + String.format(", avg: %.4f", getAvg())
                + String.format(", var: %.4f", getVar())
                + String.format(", scv: %.4f", getScv());
    }

    String toCMVStringShort() {
        return "cnt:" + getCount()
                + String.format(",avg:%.2f", getAvg())
                + String.format(",scv:%.2f", getScv());
    }
}
