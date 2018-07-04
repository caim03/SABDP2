package operators.aggregator;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class StringConcatAgg implements AggregateFunction<Tuple3<String,Integer, Integer>,Tuple2<String,String>,String> {
    @Override
    public Tuple2<String,String> createAccumulator() {
        return new Tuple2<>("","");
    }

    @Override
    public Tuple2<String, String> add(Tuple3<String, Integer, Integer> value, Tuple2<String,String> accumulator) {
        return new Tuple2<>(value.f0, accumulator.f1+=" , "+ value.f2 + "_" + value.f1);
    }

    @Override
    public String getResult(Tuple2<String,String> accumulator) {
        return accumulator.f0 + accumulator.f1;
    }

    @Override
    public Tuple2<String,String> merge(Tuple2<String,String> a, Tuple2<String,String> b) {
        return new Tuple2<>(a.f0,a.f1 + b.f1);
    }
}
