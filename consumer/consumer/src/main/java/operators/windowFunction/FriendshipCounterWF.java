package operators.windowFunction;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class FriendshipCounterWF extends ProcessWindowFunction<Integer, Tuple3<String, Integer, Integer>, Integer, TimeWindow> {
    @Override
    public void process(Integer key, Context context, Iterable<Integer> elements, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {

        int el = elements.iterator().next();

        long windowTime = context.window().getStart();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        String windowTimeStart = sdf.format(windowTime);

        out.collect(new Tuple3<>(windowTimeStart,key,el));

    }
}
