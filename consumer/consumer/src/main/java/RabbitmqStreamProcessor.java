import events.FriendshipEvent;
import events.FriendshipTimestampExtractor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.util.Collector;
import scala.Int;

import javax.xml.crypto.Data;
import javax.xml.crypto.KeySelectorResult;
import java.text.SimpleDateFormat;

public class RabbitmqStreamProcessor extends FlinkRabbitmq {

    public RabbitmqStreamProcessor(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting Rabbitmq Stream Processor..");

        Path path = new Path("/results/prova.out");

        if(FileSystem.getLocalFileSystem().exists(path)){
            FileSystem.getLocalFileSystem().delete(path, true);
        }

        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(rabbitmqHostname).setPort(rabbitmqPort).setUserName(rabbitmqUsername)
                .setPassword(rabbitmqPassword).setVirtualHost(rabbitmqVirtualHost)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<FriendshipEvent> dataStream = env.addSource(new RMQSource<String>(connectionConfig,
                queueName,
                new SimpleStringSchema())).map(line -> new FriendshipEvent(line));


        SingleOutputStreamOperator<String> filterDuplicates = dataStream
                //Assegna il watermarks (è una sorta di campo nascosto in cui lui controlla il tempo che scorre nel flusso)
                .assignTimestampsAndWatermarks(new FriendshipTimestampExtractor())
                //Map to (TS,id1,id2)
                .map(new MyMapper())
                //Tutto come chiave
                .<KeyedStream<Tuple3<Integer, Long, Long>,Tuple3<Integer, Long, Long>>>keyBy(0,1,2)
                //Elimina i duplicati nell'ultimo giorno (per fare la reduce serve inserirli in una finestra, chissà se si può aumentare sta finestra?)
                .timeWindow(Time.days(1))
                .reduce(new MyReducer())
                //Mappa in TS e basta
                .map(new MyMapper2()) //Mappa in Tuple3<Timeslot,id1,id2>
                /*Chiave = TS (per le timewindows bisogna sempre raggruppare per chiave, perchè praticamente è come se
                creasse una windows per ogni chiave*/
                .keyBy(new MyKey())
                .timeWindow(Time.days(1))
                //Conta le righe per ogni TS ed all'interno della finestra. Produce un: <InizioWindow,TS, somma)
                .apply(new Counter())
                //Key by inizio window (una sorta di groupby per raggruppare tutti gli inizi di finestra e printarli insieme)
                .keyBy(new WindowKey())
                .timeWindow(Time.days(1))
                //Produce la stringa di output
                .apply(new StringConcat())
                ;
//                ;
//
        DataStreamSink<String> dioporco = filterDuplicates.writeAsText("/results/prova.out").setParallelism(1);

        env.execute();

    }


    /**
     * Counts the number of rides arriving or departing.
     */
    public static class Counter implements WindowFunction<
                Integer, // input type
                Tuple3<String,Integer, Integer>, // output type
                Integer, // key type
                TimeWindow> // window type
    {


        @Override
        public void apply(
                Integer key,
                TimeWindow window,
                Iterable<Integer> values,
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

    public static class MyKey implements KeySelector<Integer,Integer>
    {


        @Override
        public Integer getKey(Integer integer) throws Exception {
            return integer;
        }
    }

    public static class WindowKey implements KeySelector<Tuple3<String,Integer,Integer>,String>
    {


        @Override
        public String getKey(Tuple3<String, Integer, Integer> tuple) throws Exception {
            return tuple.f0;
        }
    }


    public static class StringConcat implements WindowFunction<
            Tuple3<String,Integer, Integer>, // input type
            String, // output type
            String, // key type
            TimeWindow> // window type
    {


        @Override
        public void apply(
                String key,
                TimeWindow window,
                Iterable<Tuple3<String, Integer, Integer>> values,
                Collector<String> out) throws Exception {


            String concats = key;
            for(Tuple3<String,Integer,Integer> v : values)
                concats+=" , "+ v.f2 + "_" + v.f1;

            out.collect(concats);

        }
    }

    public static class MyMapper implements MapFunction<FriendshipEvent, Tuple3<Integer,Long,Long>> {

        @Override
        public Tuple3<Integer, Long,Long> map(FriendshipEvent event) throws Exception {
            return new Tuple3<>(event.getTimeSlot(),event.getUserId1(),event.getUserId2());
        }
    }

    public static class MyMapper2 implements MapFunction<Tuple3<Integer,Long,Long>,Integer>  {


        @Override
        public Integer map(Tuple3<Integer, Long, Long> tuple) throws Exception {
            return tuple.f0;
        }


    }




    public static class MyReducer implements ReduceFunction<Tuple3<Integer,Long,Long>>
    {

        @Override
        public Tuple3<Integer, Long, Long> reduce(Tuple3<Integer, Long, Long> t1, Tuple3<Integer, Long, Long> t2) throws Exception {
            return t1;
        }
    }
    
}


