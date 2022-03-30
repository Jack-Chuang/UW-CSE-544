package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> aggregate;

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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregate = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field g_field;
        if (this.gbfield == NO_GROUPING) g_field = null;
        else g_field = tup.getField(this.gbfield);
        if (this.what != Op.COUNT) throw new IllegalArgumentException();
        else {
            if (!this.aggregate.containsKey(g_field))
                this.aggregate.put(g_field, 1);
            else {
                int old_count = this.aggregate.get(g_field);
                this.aggregate.put(g_field, old_count + 1);
            }
        }
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
//        throw new UnsupportedOperationException("please implement me for lab2");
        return new OpIterator() {
            private ArrayList<Tuple> outputs = new ArrayList<Tuple>();
            int curr_index;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                curr_index = 0;
                if (gbfield == NO_GROUPING) {
                    TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
                    Tuple curr = new Tuple(td);
                    curr.setField(0, new IntField(aggregate.get(null)));
                    outputs.add(curr);
                }
                else {
                    for (Field curr_field: aggregate.keySet()) {
                        TupleDesc td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
                        Tuple curr = new Tuple(td);
                        curr.setField(0, curr_field);
                        curr.setField(1, new IntField(aggregate.get(curr_field)));
                        outputs.add(curr);
                    }
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return curr_index < outputs.size();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (hasNext()) {
                    Tuple tuple = outputs.get(curr_index);
                    curr_index ++;
                    return tuple;
                }
                else throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                curr_index = 0;
            }

            @Override
            public TupleDesc getTupleDesc() {
                return outputs.get(curr_index).getTupleDesc();
            }

            @Override
            public void close() {
                curr_index = 0;
                outputs = null;
            }
        };
    }

}
