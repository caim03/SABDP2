package operators.keyBy;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * Created by Caim03 on 25/06/18.
 */
public class KeyByUser implements KeySelector<Long, Long> {
    @Override
    public Long getKey(Long key) throws Exception {
        return key;
    }
}
