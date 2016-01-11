package resa.optimize;

/**
 * Created by Tom.fu on 23/4/2014.
 */
public class ServiceNode {

    public static enum ServiceType {EXPONENTIAL, DETERMINISTIC, UNIFORM, OTHER}

    private double lambda;
    private double mu;
    private ServiceType type = ServiceType.OTHER;
    private double i2oRatio;

    public ServiceNode(double l, double m, ServiceType t, double r) {
        lambda = l;
        if (Double.compare(m, 0.0) == 0) {
            throw new IllegalArgumentException("mu cannot be 0");
        }
        mu = m;
        type = t;
        i2oRatio = r;
    }

    public void setLambda(double value) {
        this.lambda = value;
    }

    public double getLambda() {
        return this.lambda;
    }

    public void setMu(double value) {
        this.mu = value;
    }

    public double getMu() {
        return this.mu;
    }

    public void setO2IRatio(double value) {
        this.i2oRatio = value;
    }

    public double getI2oRatio() {
        return this.i2oRatio;
    }


    public String getServiceTypeString() {
        return type.name().toLowerCase();
    }

    public int getMinReqServerCount() {
        return getMinReqServerCount(this.lambda, this.mu);
    }

    public double getRho(int serverCount) {
        return getRho(this.lambda, this.mu, serverCount);
    }

    public boolean isStable(int serverCount) {
        return isStable(this.lambda, this.mu, serverCount);
    }

    public double getRhoSingleServer() {
        return getRhoSingleServer(this.lambda, this.mu);
    }

    public double estErlangT(int serverCount) {
        return estErlangT(this.lambda, this.mu, serverCount);
    }

    public double estMM1T(int serverCount) {
        return estMM1T(this.lambda, this.mu, serverCount);
    }

    public String serviceNodeKeyStats(int serverCount, boolean showMinRequirement) {
        if (showMinRequirement) {
            return String.format("lambda: %.3f, mu: %.3f, rho: %.3f, k: %d, MinRequire: %d", lambda, mu,
                    getRho(serverCount), serverCount, getMinReqServerCount());
        } else {
            return String.format("lambda: %.3f, mu: %.3f, rho: %.3f, k: %d", lambda, mu, getRho(serverCount), serverCount);
        }
    }

    ///static function implementation!

    /**
     * if mu = 0.0 or serverCount not positive, then rho is not defined, we consider it as the unstable case (represented by Double.MAX_VALUE)
     * otherwise, return the calculation results. Leave the interpretation to the calling function, like isStable();
     *
     * @param lambda
     * @param mu
     * @param serverCount
     * @return
     */
    private static double getRho(double lambda, double mu, int serverCount) {
        return (mu == 0.0 || serverCount <= 0) ? Double.MAX_VALUE : lambda / (mu * (double) serverCount);
    }

    /**
     * First call getRho,
     * then determine when rho is validate, i.e., rho < 1.0
     * otherwise return unstable (FALSE)
     *
     * @param lambda
     * @param mu
     * @param serverCount
     * @return
     */
    public static boolean isStable(double lambda, double mu, int serverCount) {
        return getRho(lambda, mu, serverCount) < 1.0;
    }

    private static double getRhoSingleServer(double lambda, double mu) {
        return getRho(lambda, mu, 1);
    }

    public static int getMinReqServerCount(double lambda, double mu) {
        return (int) (lambda / mu) + 1;
    }


    /**
     * First Check isStable(), if it is stable, return the validate estimated erlang delay
     * else, return Double.Max_VALUE
     *
     * @param lambda,     average arrival rate
     * @param mu,         average execute rate
     * @param serverCount
     * @return
     */
    public static double estErlangT(double lambda, double mu, int serverCount) {
        if (isStable(lambda, mu, serverCount)) {
            double r = lambda / (mu * (double) serverCount);
            double kr = lambda / mu;

            double phi0_p1 = 1.0;
            for (int i = 1; i < serverCount; i++) {
                double a = Math.pow(kr, i);
                double b = (double) factorial(i);
                phi0_p1 += (a / b);
            }

            double phi0_p2_nor = Math.pow(kr, serverCount);
            double phi0_p2_denor = (1.0 - r) * (double) (factorial(serverCount));
            double phi0_p2 = phi0_p2_nor / phi0_p2_denor;

            double phi0 = 1.0 / (phi0_p1 + phi0_p2);

            double pWait = phi0_p2 * phi0;

            double estT = pWait * r / ((1.0 - r) * lambda) + 1.0 / mu;

            return estT;

        } else {
            //System.out.println("Service is not stable!!!");
            return Double.MAX_VALUE;
        }
    }

    public static double estMM1T(double lambda, double mu, int serverCount) {
        if (isStable(lambda, mu, serverCount)) {
            return 1.0 / (mu * (double) serverCount - lambda);
        } else {
            System.out.println("Service is not stable!!!");
            return Double.MAX_VALUE;
        }
    }

    private static int factorial(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("Attention, negative input is not allowed: " + n);
        } else if (n == 0) {
            return 1;
        } else {
            int ret = 1;
            for (int i = 2; i <= n; i++) {
                ret = ret * i;
            }
            return ret;
        }
    }
}
