package operators.aggregator;

import org.apache.flink.api.common.functions.AggregateFunction;

public class FriendShipCounterAgg implements AggregateFunction<Integer, Integer, Integer> {
    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(Integer value, Integer accumulator) {
        return accumulator +1 ;
    }

    @Override
    public Integer getResult(Integer accumulator) {
        return accumulator;
    }

    @Override
    public Integer merge(Integer a, Integer b) {
        return a+b;
    }
}
