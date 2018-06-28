package operators.apply;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * Created by Caim03 on 25/06/18.
 */
public class JoinCounter implements WindowFunction<Tuple3<Long, Long, Long>,
        Tuple3<Long, Long, Long>, Tuple2<Long, Long>, TimeWindow> {
    @Override
    public void apply(Tuple2<Long, Long> key, TimeWindow timeWindow, Iterable<Tuple3<Long, Long, Long>> values, Collector<Tuple3<Long, Long, Long>> out) throws Exception {
        /*SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        String windowTimeStart = sdf.format(key.f0);*/

        long sum = 0;
        for(Tuple3<Long, Long, Long> v : values){
            sum += v.f2;
        }

        out.collect(new Tuple3<>(key.f0, key.f1, sum));
    }
}
