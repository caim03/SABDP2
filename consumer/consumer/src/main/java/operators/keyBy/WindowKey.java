package operators.keyBy;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;


/**
 * Return a String as key (Start of window)
 */
public class WindowKey implements KeySelector<Tuple3<String,Integer,Integer>,String>
{
    @Override
    public String getKey(Tuple3<String, Integer, Integer> tuple) throws Exception {
        return tuple.f0;
    }
}