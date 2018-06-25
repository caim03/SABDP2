package operators.keyBy;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by Caim03 on 25/06/18.
 */
public class KeyByPostId implements KeySelector<Tuple2<Long, Long>, Long> {
    @Override
    public Long getKey(Tuple2<Long, Long> tupla) throws Exception {
        return tupla.f0;
    }
}
