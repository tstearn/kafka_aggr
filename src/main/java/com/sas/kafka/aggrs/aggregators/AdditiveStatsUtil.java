package com.sas.kafka.aggrs.aggregators;


import com.sas.kafka.aggrs.domain.AdditiveStatistics;

/**
 * Implements Online Algorithm for incremental calculation of variance
 */
public class AdditiveStatsUtil{
    public static  AdditiveStatistics advance(double newVal, AdditiveStatistics accumulator) {
        if(accumulator.getCount() == null) {
            initializeAccumulator(accumulator);
        }
        accumulator.setCount(accumulator.getCount()+1);
        accumulator.setSum(accumulator.getSum() + newVal);
        double delta = newVal - accumulator.getAverage();
        accumulator.setAverage(accumulator.getAverage() + delta/accumulator.getCount());
        double delta2 = newVal - accumulator.getAverage();
        accumulator.setM2(accumulator.getM2() + delta*delta2);
        return accumulator;
    }

    private static void initializeAccumulator(AdditiveStatistics accumulator) {
        accumulator.setCount(0.0);
        accumulator.setSum(0.0);
        accumulator.setMin(0.0);
        accumulator.setMax(0.0);
        accumulator.setAverage(0.0);
        accumulator.setM2(0.0);
        accumulator.setVariance(0.0);
        accumulator.setStddev(0.0);
    }

    public static double calcVariance(AdditiveStatistics accumulator) {
        return accumulator.getM2()/(accumulator.getCount() -1);
    }

    public static double calcStdDev(AdditiveStatistics accumulator) {
        return Math.sqrt(calcVariance(accumulator));
    }
}

