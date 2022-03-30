package simpledb;

import java.util.*;

import static java.lang.Integer.parseInt;
import static simpledb.Type.INT_TYPE;
import static simpledb.Type.STRING_TYPE;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
//    private int index1;
//    private int index2;
    private ConcurrentHashMap<Field, ArrayList<Tuple>> hash_table;
    private int which_is_hash = 1;
//    private List<Tuple> sorted1 = Collections.synchronizedList(new ArrayList<Tuple>());
//    private List<Tuple> sorted2 = Collections.synchronizedList(new ArrayList<Tuple>());
//    private List<Tuple> output_buffer = Collections.synchronizedList(new ArrayList<Tuple>());
    private ConcurrentLinkedQueue<Tuple> outputBuffer = new ConcurrentLinkedQueue<Tuple>();

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return this.child1.getTupleDesc().getFieldName(this.p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return this.child2.getTupleDesc().getFieldName(this.p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(this.child1.getTupleDesc(), this.child2.getTupleDesc());
    }

    // Implements comparable interface into custom class used for sorting the arraylists
    class TupleComparator implements Comparator<Tuple> {

        private int i1;
        private int i2;
        private String s1;
        private String s2;
        public boolean isInt;

        // function to change type of field
        public void change(Field f1, Field f2) {
            if (f1.getType() == STRING_TYPE) {
                this.s1 = f1.toString();
                isInt = false;
            }
            if (f1.getType() == INT_TYPE) {
                this.i1 = parseInt(f1.toString());
                isInt = true;
            }
            if (f2.getType() == STRING_TYPE) this.s2 = f2.toString();
            if (f2.getType() == INT_TYPE) this.i2 = parseInt(f2.toString());
        }

        // Function to compare
        public int compare(Tuple t1, Tuple t2)
        {
            Field f1 = t1.getField(Join.this.p.getField1());
            Field f2 = t2.getField(Join.this.p.getField2());
            change(f1, f2);
            if (isInt) {
                if (this.i1 == this.i2)
                    return 0;
                else if (this.i1 > this.i2)
                        return 1;
                    else
                        return -1;
            }
            // temporarily comparison method
            else {
                if (this.s1.length() == this.s2.length())
                    return 0;
                else if (this.s1.length() > this.s2.length())
                    return 1;
                else
                    return -1;
            }
        }
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.child1.open();
        this.child2.open();

        // First attempt: used hash join but is useless when encounter inequalities
        OpIterator hash_table_builder;
        int child1_num = 0;
        while (this.child1.hasNext()) {
            child1_num ++;
            this.child1.next();
        }
        int child2_num = 0;
        while (this.child2.hasNext()) {
            child2_num ++;
            this.child2.next();
        }
        this.hash_table = new ConcurrentHashMap<>();
        this.outputBuffer = new ConcurrentLinkedQueue<>();

        // let the smaller table be the hash table
        if (child1_num > child2_num) {
            hash_table_builder = this.child2;
            this.which_is_hash = 2;
        }
        else {
            hash_table_builder = this.child1;
            this.which_is_hash = 1;
        }

        this.child1.rewind();
        this.child2.rewind();
        // start comparing tuples
        OpIterator iterator;
        if (this.which_is_hash == 1) iterator = this.child2;
        else iterator = this.child1;

        String predicate = this.p.getOperator().toString();
        if (predicate.equals("=")) {
            // build the hash table
            while (hash_table_builder.hasNext()) {
                // hash method: every tuple with the same field value is in the same bucket
                Tuple t = hash_table_builder.next();
                Field f;
                if (this.which_is_hash == 1) f = t.getField(this.p.getField1());
                else f = t.getField(this.p.getField2());
                if (this.hash_table.isEmpty() || !this.hash_table.containsKey(f)) this.hash_table.put(f, new ArrayList<Tuple>());
                this.hash_table.get(f).add(t);
            }

            while (iterator.hasNext()) {
                Tuple t1 = iterator.next();
                Field f;
                if (this.which_is_hash == 1) f = t1.getField(this.p.getField2());
                else f = t1.getField(this.p.getField1());
                ArrayList<Tuple> compare = this.hash_table.get(f);
                if (compare != null) {
                    for (Tuple t2: compare) {
                        if (this.p.filter(t1, t2)) {
                            Tuple merged_tuple = new Tuple(TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc()));
                            for (int i = 0; i < t2.getTupleDesc().numFields(); i++) {
                                merged_tuple.setField(i, t2.getField(i));
                            }
                            for (int i = 0; i < t1.getTupleDesc().numFields(); i++) {
                                merged_tuple.setField(i + t2.getTupleDesc().numFields(), t1.getField(i));
                            }
                            this.outputBuffer.add(merged_tuple);
                        }
                    }
                }
            }
        } else {
            while (this.child1.hasNext()) {
                Tuple t1 = this.child1.next();
                while (this.child2.hasNext()) {
                    Tuple t2 = this.child2.next();
                    if (this.p.filter(t1, t2)) {
                        Tuple merged_tuple = new Tuple(TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc()));
                        for (int i = 0; i < t1.getTupleDesc().numFields(); i++) {
                            merged_tuple.setField(i, t1.getField(i));
                        }
                        for (int i = 0; i < t2.getTupleDesc().numFields(); i++) {
                            merged_tuple.setField(i + t1.getTupleDesc().numFields(), t2.getField(i));
                        }
                        this.outputBuffer.add(merged_tuple);
                    }
                }
                this.child2.rewind();
            }
        }

        // sort both table upon open
//        this.index1 = 0;
//        this.index2 = 0;
//        while (this.child1.hasNext()) {
//            this.sorted1.add(this.child1.next());
//        }
//        while (this.child2.hasNext()) {
//            this.sorted2.add(this.child2.next());
//        }
//        Collections.sort(this.sorted1, new TupleComparator());
//        Collections.sort(this.sorted2, new TupleComparator());
//        String predicate = this.p.getOperator().toString();
//        if (predicate.equals("=")) {
//            while (this.index1 < this.sorted1.size()) {
//                Tuple t1 = this.sorted1.get(this.index1);
//                while (this.index2 < this.sorted2.size()) {
//                    Tuple t2 = this.sorted2.get(this.index2);
//                    if (this.p.filter(t1, t2)) {
//                        Tuple merged_tuple = new Tuple(TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc()));
//                        for (int i = 0; i < t1.getTupleDesc().numFields(); i++) {
//                            merged_tuple.setField(i, t1.getField(i));
//                        }
//                        for (int i = 0; i < t2.getTupleDesc().numFields(); i++) {
//                            merged_tuple.setField(i + t1.getTupleDesc().numFields(), t2.getField(i));
//                        }
//                        this.outputBuffer.add(merged_tuple);
//                    }
//                    this.index2++;
//                }
//                this.index1++;
//                this.index2 = 0;
//            }
//        } else {
//            while (this.index1 < this.sorted1.size()) {
//                Tuple t1 = this.sorted1.get(this.index1);
//                TupleDesc t1_td = t1.getTupleDesc();
//                for (int i = 0; i < this.sorted2.size(); i++) {
//                    if (this.sorted2.get(i).getTupleDesc().equals(t1_td)) this.index2 = i;
//                }
//                int front;
//                int back;
//                if (predicate.equals(">=") || predicate.equals(">")) {
//                    front = this.index2;
//                    back = this.sorted2.size();
//                } else {
//                    front = 0;
//                    back = this.index2;
//                }
//                for (int i = front; i < back; i++) {
//                    Tuple t2 = this.sorted2.get(i);
//                    if (this.p.filter(t1, t2)) {
//                        Tuple merged_tuple = new Tuple(TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc()));
//                        for (int j = 0; j < t1.getTupleDesc().numFields(); j++) {
//                            merged_tuple.setField(j, t1.getField(j));
//                        }
//                        for (int j = 0; j < t2.getTupleDesc().numFields(); j++) {
//                            merged_tuple.setField(j + t1.getTupleDesc().numFields(), t2.getField(j));
//                        }
//                        this.output_buffer.add(merged_tuple);
//                    }
//                }
//                this.index1 ++;
//            }
//        }
    }

    public void close() {
        // some code goes here
        super.close();
        this.child1.close();
        this.child2.close();
//        this.index1 = 0;
//        this.index2 = 0;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
//        this.child1.rewind();
//        this.child2.rewind();
//        this.index1 = 0;
//        this.index2 = 0;
        this.close();
        this.open();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // First attempt: used hash join but is useless when encounter inequalities
//        if (!this.outputBuffer.isEmpty()) return this.outputBuffer.poll();
//        OpIterator input_comparing;
//        // figure out which one is the other table not used for building hash table
//        if (this.which_is_hash == 1) input_comparing = this.child2;
//        else input_comparing = this.child1;
//        while (input_comparing.hasNext()) {
//            Tuple t_compare = input_comparing.next();
//            Field f_compare;
//            if (input_comparing == this.child1) f_compare = t_compare.getField(this.p.getField1());
//            else f_compare = t_compare.getField(this.p.getField2());
//            if (this.hash_table.containsKey(f_compare)) {
//                ArrayList<Tuple> same_hash = this.hash_table.get(f_compare);
//                for (Tuple same_field_tuple: same_hash) {
//                    if (this.p.filter(same_field_tuple, t_compare)) {
//                        TupleDesc td_same = same_field_tuple.getTupleDesc();
//                        TupleDesc td_compare = t_compare.getTupleDesc();
//                        Tuple merged_tuple = new Tuple(TupleDesc.merge(td_same, td_compare));
//                        for (int i = 0; i < td_same.numFields(); i++){
//                            merged_tuple.setField(i, same_field_tuple.getField(i));
//                        }
//                        for (int i = 0; i < td_compare.numFields(); i++){
//                            merged_tuple.setField(i + td_same.numFields(), t_compare.getField(i));
//                        }
//                        this.outputBuffer.offer(merged_tuple);
//                    }
//                }
//            }
//            return this.outputBuffer.poll();
//        }

        // if there's still something in the outputbuffer, print it
        if (this.outputBuffer.isEmpty()) return null;
        return this.outputBuffer.poll();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child1, this.child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
