package com.simpledb.optimizer;

import com.simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;

    private int min;

    private int max;

    private double width;

    private int[] bucket;

    private int count;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.width = (double) (max - min) / buckets;
        this.bucket = new int[buckets];
        this.count = 0;
    }

    private int getBucketIndex(int v) {
        return v == max ? (buckets - 1) : (int) ((v - min) / width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v < min || v > max) return;
        bucket[getBucketIndex(v)]++;
        count++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        double selectivity = 0;
        switch (op) {
            case EQUALS -> {
                // (h / w) / ntups
                if (v < min || v > max) return 0;
                selectivity = bucket[getBucketIndex(v)] / Math.ceil(width) / count;
            }
            case NOT_EQUALS -> {
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            }
            case GREATER_THAN -> {
                if (v > max) return 0;
                if (v < min) return 1;
                for (int i = getBucketIndex(v) + 1; i < buckets; i++) {
                    selectivity += bucket[i];
                }
                selectivity += ((v % width) / width) * bucket[getBucketIndex(v)];
                selectivity /= count;
            }
            case GREATER_THAN_OR_EQ -> {
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            }
            case LESS_THAN -> {
                if (v > max) return 1;
                if (v < min) return 0;
                for (int i = 0; i < getBucketIndex(v); i++) {
                    selectivity += bucket[i];
                }
                selectivity += ((v % width) / width) * bucket[getBucketIndex(v)];
                selectivity /= count;
            }
            case LESS_THAN_OR_EQ -> {
                return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            }
        }
        return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        double selectivity = 0;
        for (int i : bucket) {
            selectivity += (double) i / count;
        }
        return selectivity / buckets;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
