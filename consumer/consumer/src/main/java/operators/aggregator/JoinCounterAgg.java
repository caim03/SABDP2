package operators.aggregator;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class JoinCounterAgg implements AggregateFunction<Tuple3<Long, Long, Long>,Tuple3<Long, Long, Long>,Tuple3<Long, Long, Long>> {
    @Override
    public Tuple3<Long, Long, Long> createAccumulator() {
        return new Tuple3<>(0L,0L,0L);
    }

    @Override
    public Tuple3<Long, Long, Long> add(Tuple3<Long, Long, Long> value, Tuple3<Long, Long, Long> accumulator) {
        return new Tuple3<>(value.f0,value.f1, value.f2 + accumulator.f2);
    }

    @Override
    public Tuple3<Long, Long, Long> getResult(Tuple3<Long, Long, Long> accumulator) {
        return accumulator;
    }

    @Override
    public Tuple3<Long, Long, Long> merge(Tuple3<Long, Long, Long> a, Tuple3<Long, Long, Long> b) {
        return new Tuple3<>(a.f0,b.f1,a.f2 + b.f2);
    }
}
