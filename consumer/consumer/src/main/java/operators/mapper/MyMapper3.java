package operators.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;


/**
 * Map a Tuple3 objects Integer (Time Slot of current record)
 * */
public class MyMapper3 implements MapFunction<Tuple3<Integer,Long,Long>,Tuple3<Integer, Long, Long>> {

    @Override
    public Tuple3<Integer, Long, Long> map(Tuple3<Integer, Long, Long> tuple) throws Exception {
        return new Tuple3<>(tuple.f0, 1L, tuple.f1);
    }
}
