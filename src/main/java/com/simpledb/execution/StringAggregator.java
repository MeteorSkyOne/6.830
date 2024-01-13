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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static IntField NO_GROUPING_FIELD = new IntField(-1);

    private int gbFieldIdx;

    private Type gbFieldType;

    private int aField;

    private Op op;

    private Map<Field, Tuple> aggrMap;


    private TupleDesc td;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbFieldIdx = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.aggrMap = new HashMap<>();
        if (gbfield == NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
        } else {
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupValue", "aggregateValue"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.gbFieldIdx == NO_GROUPING) {
            aggrMap.compute(NO_GROUPING_FIELD, this::calculate);
        } else {
            aggrMap.compute(tup.getField(gbFieldIdx), this::calculate);
        }
    }

    private Tuple calculate(Field gbField, Tuple vTuple) {
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
        }
        return res;
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggregator.StringAggregateIterator(this);
    }

    private class StringAggregateIterator implements OpIterator {

        private StringAggregator aggregator;

        private Iterator<Tuple> it;

        public StringAggregateIterator(StringAggregator aggregator) {
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
