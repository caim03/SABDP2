package operators.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;


/**
 * Map a Tuple3 objects Integer (Time Slot of current record)
 * */
public class TimeSlotMapper implements MapFunction<Tuple3<Integer,Long,Long>,Integer> {

    @Override
    public Integer map(Tuple3<Integer, Long, Long> tuple) throws Exception {
        return tuple.f0;
    }
}
