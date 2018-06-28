package operators.keyBy;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by Caim03 on 25/06/18.
 */
public class KeyByWindowStart implements KeySelector<Tuple3<Long, Long, Long>, Long> {
    @Override
    public Long getKey(Tuple3<Long, Long, Long> tupla) throws Exception {
        return tupla.f0;
    }
}
