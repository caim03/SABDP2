package operators.keyBy;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by Caim03 on 25/06/18.
 */
public class JoinKey implements KeySelector<Tuple3<Long, Long, Long>, Tuple2<Long, Long>> {
    @Override
    public Tuple2<Long, Long> getKey(Tuple3<Long, Long, Long> tupla) throws Exception {
        return new Tuple2<>(tupla.f0, tupla.f1);
    }
}
