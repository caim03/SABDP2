package operators.trigger;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * Created by Caim03 on 26/06/18.
 */
public class UserTrigger extends Trigger<Tuple3<Integer, Long, Long>, GlobalWindow> {
    @Override
    public TriggerResult onElement(Tuple3<Integer, Long, Long> tupla, long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
        if(tupla.f2 == -1){
            return TriggerResult.FIRE;
        }
        else {
            return TriggerResult.CONTINUE;
        }

    }

    @Override
    public TriggerResult onProcessingTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {

    }
}
