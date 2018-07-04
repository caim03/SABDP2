package operators.aggregator;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/*
*  <IN,AGG,OUT>
* */
public class PostCounterAgg implements AggregateFunction<Tuple2<Long,Long>,Long,Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Tuple2<Long, Long> value, Long accumulator) {
        return accumulator+1L;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a+b;
    }
}
