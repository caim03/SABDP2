package operators.apply;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.TreeSet;

/**
 * Created by Caim03 on 25/06/18.
 */
public class Ranking implements WindowFunction<Tuple3<Long, Long, Long>,
        String,
        Long,
        TimeWindow>{
    @Override
    public void apply(Long key, TimeWindow timeWindow, Iterable<Tuple3<Long, Long, Long>> values, Collector<String> out) throws Exception {
        Comparator<Tuple2<Long, Long>> comparator = (tupla1, tupla2) -> (tupla1.f1.compareTo(tupla2.f1));
        TreeSet<Tuple2<Long, Long>> tree = new TreeSet<>(comparator);

        for(Tuple3<Long, Long, Long> v : values){
            tree.add(new Tuple2<>(v.f1, v.f2));
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        String concat = sdf.format(key);

        long size = Math.min(10, tree.size());

        for(int i = 0; i < size; i++){
            Tuple2<Long, Long> ranked = tree.pollLast();
            concat += " , " + ranked.f0 + " , " + ranked.f1;
        }

        out.collect(concat);
    }
}

