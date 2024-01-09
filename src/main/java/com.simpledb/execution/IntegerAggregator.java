package com.simpledb.execution;

import com.simpledb.common.DbException;
import com.simpledb.common.Type;
import com.simpledb.storage.Field;
import com.simpledb.storage.IntField;
import com.simpledb.storage.Tuple;
import com.simpledb.storage.TupleDesc;
import com.simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static IntField NO_GROUPING_FIELD = new IntField(-1);

    private int gbFieldIdx;

    private Type gbFieldType;

    private int aField;

    private Op op;

    private Map<Field, Tuple> aggrMap;

    private Map<Field, Tuple> avgMap;

    private TupleDesc td;

    private TupleDesc avgTd;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbFieldIdx = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.aggrMap = new HashMap<>();
        this.avgMap = new HashMap<>();
        if (gbfield == NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
        } else {
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupValue", "aggregateValue"});
        }
        this.avgTd = new TupleDesc(new Type[]{Type.INT_TYPE, Type.INT_TYPE}, new String[]{"count", "sum"});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.gbFieldIdx == NO_GROUPING) {
            aggrMap.compute(NO_GROUPING_FIELD, (k, v) -> calculate((IntField) tup.getField(aField), k, v));
        } else {
            aggrMap.compute(tup.getField(gbFieldIdx), (k, v) -> calculate((IntField) tup.getField(aField), k, v));
        }
    }

    private Tuple calculate(IntField aggregateField, Field gbField, Tuple vTuple) {
        Tuple res = new Tuple(td);
        int valueIdx;
        if (gbFieldIdx == NO_GROUPING) {
            valueIdx = 0;
        } else {
            valueIdx = 1;
            res.setField(0, gbField);
        }
        IntField v = vTuple == null ? null : (IntField) vTuple.getField(valueIdx);
        switch (op) {
            case COUNT:
                res.setField(valueIdx, new IntField(v == null ? 1 : v.getValue() + 1));
                break;
            case SUM:
                res.setField(valueIdx, new IntField(v == null ? aggregateField.getValue() : v.getValue() + aggregateField.getValue()));
                break;
            case AVG:
                avgMap.compute(gbField, (k, avgV) -> {
                    Tuple avgRes = new Tuple(avgTd);
                    if (avgV == null) {
                        avgRes.setField(0, new IntField(1));
                        avgRes.setField(1, new IntField(aggregateField.getValue()));
                    } else {
                        avgRes.setField(0, new IntField(((IntField) avgV.getField(0)).getValue() + 1));
                        avgRes.setField(1, new IntField(((IntField) avgV.getField(1)).getValue() + aggregateField.getValue()));
                    }
                    return avgRes;
                });
                Tuple avgTuple = avgMap.get(gbField);
                IntField count = (IntField) avgTuple.getField(0);
                IntField sum = (IntField) avgTuple.getField(1);
                res.setField(valueIdx, new IntField(sum.getValue() / count.getValue()));
                break;
            case MIN:
                if (v == null) {
                    res.setField(valueIdx, new IntField(aggregateField.getValue()));
                } else {
                    res.setField(valueIdx, (v.compare(Predicate.Op.LESS_THAN, aggregateField)) ? v : aggregateField);
                }
                break;
            case MAX:
                if (v == null) {
                    res.setField(valueIdx, new IntField(aggregateField.getValue()));
                } else {
                    res.setField(valueIdx, (v.compare(Predicate.Op.GREATER_THAN, aggregateField)) ? v : aggregateField);
                }
                break;
        }
        return res;
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntAggregateIterator(this);
    }

    private class IntAggregateIterator implements OpIterator {

        private IntegerAggregator aggregator;

        private Iterator<Tuple> it;

        public IntAggregateIterator(IntegerAggregator aggregator) {
            this.aggregator = aggregator;
            it = null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = aggregator.aggrMap.values().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggregator.td;
        }

        @Override
        public void close() {
            it = null;
        }
    }

}
