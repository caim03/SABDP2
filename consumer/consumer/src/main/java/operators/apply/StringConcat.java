package operators.apply;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Collects the output stream in this way: Date (key window), count_timeSlot
 * */
public class StringConcat implements WindowFunction
        <Tuple3<String,Integer, Integer>, // input type
        String, // output type
        String, // key type
        TimeWindow> // window type
{

    @Override
    public void apply(String key, TimeWindow window, Iterable<Tuple3<String, Integer, Integer>> values,
            Collector<String> out) throws Exception {


        String concats = key;
        for(Tuple3<String,Integer,Integer> v : values)
            concats+=" , "+ v.f2 + "_" + v.f1;

        out.collect(concats);
    }
}