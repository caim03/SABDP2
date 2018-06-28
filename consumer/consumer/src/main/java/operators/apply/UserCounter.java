package operators.apply;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by Caim03 on 25/06/18.
 */
public class UserCounter implements WindowFunction<Long, Tuple3<Long, Long, Long>, Long, TimeWindow> {
    @Override
    public void apply(Long key, TimeWindow timeWindow, Iterable<Long> values, Collector<Tuple3<Long, Long, Long>> out) throws Exception {

        long windowTime = timeWindow.getStart();

        long counter = 0;
        for(Long v : values)
            counter++;

        out.collect(new Tuple3<>(windowTime,key,counter));
    }
}
