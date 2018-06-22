package operators.reducer;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class ReduceDuplicate implements ReduceFunction<Tuple3<Integer,Long,Long>>
{

    @Override
    public Tuple3<Integer, Long, Long> reduce(Tuple3<Integer, Long, Long> t1, Tuple3<Integer, Long, Long> t2) throws Exception {
        return t1;
    }
}
