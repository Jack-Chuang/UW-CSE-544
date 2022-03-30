package simpledb;

import java.util.*;

import static java.lang.Integer.parseInt;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
//    private HashMap<Field, ArrayList<Integer>> groups;
    private HashMap<Field, Integer> integer_aggregate;
    private HashMap<Field, Integer> integer_count;
    private HashMap<Field, Integer> integer_sum;

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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
//        this.groups = new HashMap<>();
        this.integer_aggregate = new HashMap<>();
        this.integer_count = new HashMap<>();
        this.integer_sum = new HashMap<>();
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
        Field g_field;
        int a_field = Integer.parseInt(tup.getField(this.afield).toString());
        int old_max, old_min, old_sum, old_count, new_sum, count, avg;
        if (this.gbfield == NO_GROUPING) g_field = null;
        else g_field = tup.getField(this.gbfield);
        switch (this.what) {
            case MAX:
                if (!this.integer_aggregate.containsKey(g_field))
                    this.integer_aggregate.put(g_field, a_field);
                else {
                    old_max = this.integer_aggregate.get(g_field);
                    this.integer_aggregate.put(g_field, Math.max(old_max, a_field));
                }
                break;
            case MIN:
                if (!this.integer_aggregate.containsKey(g_field))
                    this.integer_aggregate.put(g_field, a_field);
                else {
                    old_min = this.integer_aggregate.get(g_field);
                    this.integer_aggregate.put(g_field, Math.min(old_min, a_field));
                }
                break;
            case SUM:
                if (!this.integer_aggregate.containsKey(g_field))
                    this.integer_aggregate.put(g_field, a_field);
                else {
                    old_sum = this.integer_aggregate.get(g_field);
                    this.integer_aggregate.put(g_field, old_sum + a_field);
                }
                break;
            case COUNT:
                if (!this.integer_aggregate.containsKey(g_field)) {
                    this.integer_aggregate.put(g_field, 1);
                }
                else {
                    old_count = this.integer_aggregate.get(g_field);
                    this.integer_aggregate.put(g_field, old_count + 1);
                }
                break;
            case AVG:
                if (!this.integer_aggregate.containsKey(g_field)) {
                    this.integer_aggregate.put(g_field, a_field);
                    this.integer_sum.put(g_field, a_field);
                    this.integer_count.put(g_field, 1);
                }
                else {
                    //old_average = this.integer_aggregate.get(g_field);
                    //this.integer_aggregate.put(g_field, (old_average * (this.count - 1) + a_field) / this.count);
                    old_sum = this.integer_sum.get(g_field);
                    new_sum = old_sum + a_field;
                    count = this.integer_count.get(g_field);
                    count++;
                    avg = new_sum/count;
                    this.integer_count.put(g_field, count);
                    this.integer_sum.put(g_field, new_sum);
                    this.integer_aggregate.put(g_field, avg);
                }
                break;
        }

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
//        throw new
//        UnsupportedOperationException("please implement me for lab2");

        return new OpIterator() {
//            Iterator<Map.Entry<Field, Integer>> itr = integer_aggregate.entrySet().iterator();
            ArrayList<Tuple> outputs = new ArrayList<Tuple>();
            int curr_index;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                curr_index = 0;
                if (gbfield == NO_GROUPING) {
                    TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
                    Tuple curr = new Tuple(td);
                    curr.setField(0, new IntField(integer_aggregate.get(null)));
                    outputs.add(curr);
                }
                else {
                    for (Field curr_field: integer_aggregate.keySet()) {
                        TupleDesc td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
                        Tuple curr = new Tuple(td);
                        curr.setField(0, curr_field);
                        curr.setField(1, new IntField(integer_aggregate.get(curr_field)));
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
