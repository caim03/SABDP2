package operators.keyBy;

import org.apache.flink.api.java.functions.KeySelector;


/**
 * Return an Integer as key (Time Slot key)
 */
public class KeyByTimeSlot implements KeySelector<Integer,Integer>
{
    @Override
    public Integer getKey(Integer integer) throws Exception {
        return integer;
    }
}