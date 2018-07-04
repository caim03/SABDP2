package operators.aggregator;

import org.apache.flink.api.common.functions.AggregateFunction;

public class UserCounterAgg implements AggregateFunction<Long,Long,Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Long value, Long accumulator) {
        return accumulator +1;
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
