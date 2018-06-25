package operators.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by Caim03 on 25/06/18.
 */
public class PostFilter implements FilterFunction<Tuple2<Long, Long>> {

    @Override
    public boolean filter(Tuple2<Long, Long> tupla) throws Exception {
        if(tupla.f1 == -1){
            return true;
        }
        return false;
    }
}
