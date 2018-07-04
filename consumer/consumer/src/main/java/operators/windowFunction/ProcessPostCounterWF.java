package operators.windowFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
 <IN,OUT,KEY,TW>
 */
public class ProcessPostCounterWF extends ProcessWindowFunction<Long, Tuple3<Long, Long, Long>, Long, TimeWindow> {
    @Override
    public void process(Long key, Context context, Iterable<Long> elements, Collector<Tuple3<Long, Long, Long>> out) throws Exception {

        Long el = elements.iterator().next();
        long timestamp = context.window().getStart();

        out.collect(new Tuple3<>(timestamp,key,el));

    }
}
