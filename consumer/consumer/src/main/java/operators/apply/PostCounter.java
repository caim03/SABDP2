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
public class PostCounter implements WindowFunction<Tuple2<Long, Long>, // input type
        Tuple3<Long, Long, Long>, // output type
        Long, // key type
        TimeWindow> // window type
{

    @Override
    public void apply(Long key, TimeWindow timeWindow, Iterable<Tuple2<Long, Long>> values,
                      Collector<Tuple3<Long, Long, Long>> out) throws Exception {

        long timestamp = timeWindow.getStart();

        /*SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        String windowTimeStart = sdf.format(timestamp);*/

        long counter = 0;
        for(Tuple2<Long, Long> v : values)
            counter++;

        out.collect(new Tuple3<>(timestamp,key,counter));
    }
}