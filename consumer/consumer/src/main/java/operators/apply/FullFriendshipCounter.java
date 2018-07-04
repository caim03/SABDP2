package operators.apply;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * Created by Caim03 on 26/06/18.
 */
public class FullFriendshipCounter implements WindowFunction<
        Tuple3<Integer, Long, Long>,
        Tuple2<Integer, Long>,
        Integer,
        GlobalWindow> {

    @Override
    public void apply(Integer key, GlobalWindow globalWindow, Iterable<Tuple3<Integer, Long, Long>> values, Collector<Tuple2<Integer, Long>> out) throws Exception {
        long counter = 0;
        for(Tuple3<Integer, Long, Long> v : values){
            counter++;
        }

        out.collect(new Tuple2<>(key, counter));
    }
}
