package operators.evictor;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

/**
 * Created by Caim03 on 27/06/18.
 */
public class UserEvictor implements Evictor<Tuple3<Integer, Long, Long>, GlobalWindow> {

    @Override
    public void evictBefore(Iterable<TimestampedValue<Tuple3<Integer, Long, Long>>> iterable, int i, GlobalWindow globalWindow, EvictorContext evictorContext) {
        Iterator<TimestampedValue<Tuple3<Integer, Long, Long>>> iterator = iterable.iterator();
        TimestampedValue<Tuple3<Integer, Long, Long>> t = iterator.next();

        while (iterator.hasNext()){
            t = iterator.next();
        }

        iterator.remove();
    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<Tuple3<Integer, Long, Long>>> iterable, int i, GlobalWindow globalWindow, EvictorContext evictorContext) {

    }
}
