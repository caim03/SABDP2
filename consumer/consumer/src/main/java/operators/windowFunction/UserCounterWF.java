package operators.windowFunction;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.sys.process.Process;

import java.text.SimpleDateFormat;

public class UserCounterWF extends ProcessWindowFunction<Long, Tuple3<Long, Long, Long>, Long, TimeWindow> {
    @Override
    public void process(Long key, Context context, Iterable<Long> elements, Collector<Tuple3<Long, Long, Long>> out) throws Exception {

        long el = elements.iterator().next();
        long windowTime = context.window().getStart();

        out.collect(new Tuple3<>(windowTime,key,el));

    }
}
