package operators.keyBy;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by Caim03 on 26/06/18.
 */
public class MyKey2 implements KeySelector<Tuple3<Integer, Long, Long>, Integer> {
    @Override
    public Integer getKey(Tuple3<Integer, Long, Long> tupla) throws Exception {
        return tupla.f0;
    }
}
