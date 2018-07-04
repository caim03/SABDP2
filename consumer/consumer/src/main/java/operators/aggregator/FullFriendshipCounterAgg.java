package operators.aggregator;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class FullFriendshipCounterAgg implements AggregateFunction<Tuple3<Integer, Long, Long>,Tuple2<Integer,Long>,Tuple2<Integer,Long>> {
    @Override
    public Tuple2<Integer, Long> createAccumulator() {
        return new Tuple2<>(0,0L);
    }

    @Override
    public Tuple2<Integer, Long> add(Tuple3<Integer, Long, Long> value, Tuple2<Integer, Long> accumulator) {
        return new Tuple2<>(value.f0, accumulator.f1+1);
    }

    @Override
    public Tuple2<Integer, Long> getResult(Tuple2<Integer, Long> accumulator) {
        return accumulator;
    }

    @Override
    public Tuple2<Integer, Long> merge(Tuple2<Integer, Long> a, Tuple2<Integer, Long> b) {
        return new Tuple2<>(a.f0, a.f1 + b.f1);
    }
}
