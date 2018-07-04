package operators.apply;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * Counts the number of rides arriving or departing.
 */
public class FriendShipCounter implements WindowFunction
        <Integer, // input type
        Tuple3<String,Integer, Integer>, // output type
        Integer, // key type
        TimeWindow> // window type
{

    @Override
    public void apply(Integer key, TimeWindow window, Iterable<Integer> values,
            Collector<Tuple3<String, Integer, Integer>> out) throws Exception {

        int timeslot = key;
        long windowTime = window.getStart();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        String windowTimeStart = sdf.format(windowTime);


        int counter = 0;
        for(Integer v : values)
            counter++;

        out.collect(new Tuple3<>(windowTimeStart,timeslot,counter));
    }
}