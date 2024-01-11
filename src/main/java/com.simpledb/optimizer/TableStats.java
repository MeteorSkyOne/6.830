package com.simpledb.optimizer;

import com.simpledb.common.Database;
import com.simpledb.common.Type;
import com.simpledb.execution.Predicate;
import com.simpledb.execution.SeqScan;
import com.simpledb.storage.*;
import com.simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableId;

    private int ioCostPerPage;

    private int ntups;

    private int pages;

    private TupleDesc td;

    private Map<Integer, IntHistogram> intHistogramMap;

    private Map<Integer, StringHistogram> stringHistogramMap;

    private Map<Integer, Integer> minMap;

    private Map<Integer, Integer> maxMap;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        HeapFile dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.td = dbFile.getTupleDesc();
        this.pages = dbFile.numPages();
        this.intHistogramMap = new HashMap<>();
        this.stringHistogramMap = new HashMap<>();
        this.minMap = new HashMap<>();
        this.maxMap = new HashMap<>();
        SeqScan seqScan = new SeqScan(new TransactionId(), tableId);
        try {
            seqScan.open();
            while (seqScan.hasNext()) {
                // calculate min and max value in the first scan
                ntups++;
                Tuple nextTup = seqScan.next();
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i).equals(Type.INT_TYPE)) {
                        int fieldValue = ((IntField) nextTup.getField(i)).getValue();
                        minMap.compute(i, (k, v) -> v == null ? fieldValue : Math.min(v, fieldValue));
                        maxMap.compute(i, (k, v) -> v == null ? fieldValue : Math.max(v, fieldValue));
                    } else {
                        // string
                        String fieldValue = ((StringField) nextTup.getField(i)).getValue();
                        stringHistogramMap.compute(i, (k, v) -> {
                            if (v == null) {
                                StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
                                stringHistogram.addValue(fieldValue);
                                return stringHistogram;
                            } else {
                                v.addValue(fieldValue);
                                return v;
                            }
                        });
                    }
                }
            }
            seqScan.rewind();
            while (seqScan.hasNext()) {
                // add value to IntHistogram in the second scan
                Tuple nextTup = seqScan.next();
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i).equals(Type.INT_TYPE)) {
                        int fieldValue = ((IntField) nextTup.getField(i)).getValue();
                        int finalI = i;
                        intHistogramMap.compute(i, (k, v) -> {
                            if (v == null) {
                                IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, minMap.get(finalI), maxMap.get(finalI));
                                intHistogram.addValue(fieldValue);
                                return intHistogram;
                            } else {
                                v.addValue(fieldValue);
                                return v;
                            }
                        });
                    }
                }
            }
        } catch (Exception e) {

        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     *
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return 2 * ioCostPerPage * pages;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (ntups * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        if (td.getFieldType(field).equals(Type.INT_TYPE)) {
            return intHistogramMap.get(field).avgSelectivity();
        } else {
            return stringHistogramMap.get(field).avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (td.getFieldType(field).equals(Type.INT_TYPE)) {
            return intHistogramMap.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        } else {
            return stringHistogramMap.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return ntups;
    }

}
