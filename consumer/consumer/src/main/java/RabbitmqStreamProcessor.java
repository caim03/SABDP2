import events.FriendshipEvent;
import events.FriendshipTimestampExtractor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import javax.xml.crypto.Data;

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



        DataStreamSink<Tuple2<Integer, Long>> filterDuplicates = dataStream
                .assignTimestampsAndWatermarks(new FriendshipTimestampExtractor())
                .map(new MyMapper())
                .<KeyedStream<Tuple3<Integer, Long, Long>,Tuple3<Integer, Long, Long>>>keyBy(0,1,2)
                .timeWindow(Time.hours(24))
                .reduce(new MyReducer())
                .map(new MyMapper2())
                .keyBy(0)
                .sum(1)
                .writeAsText("/results/prova.out").setParallelism(1);
                ;




//        WindowedStream windowedStream = dataStream
//                .map(event -> new Tuple3<>(event.getTimeSlot(),event.getUserId1(),event.getUserId2()))
//                .<KeyedStream<Tuple3<Integer, Long, Long>,Tuple3<Integer, Long, Long>>>keyBy(0,1,2)
//                .timeWindow(Time.hours(24));

        /*DataStreamSink<Tuple2<Integer, Long>> counters = windowedStream
                .reduce((tupla1, tupla2) -> tupla1)
                .flatMap((FlatMapFunction<Tuple3<Integer, Long, Long>, Tuple2<Integer, Long>>) (event, collector) -> {
                    collector.collect(new Tuple2<>(event.f0, 1L));
                })
                .keyBy(0)
                .sum(1).writeAsText("/results/prova.out").setParallelism(1);*/

                //.setParallelism(1).writeAsText("/results/prova.out");
                /*.keyBy(0,1,2)
                .reduce((tuple1, tuple2) -> tuple1)
                .keyBy(0)
                .fold(new Tuple2<>(0, 0L),
                        (FoldFunction<Tuple3<Integer, Long, Long>, Tuple2<Integer, Long>>)
                                (current, stream) -> new Tuple2<>(stream.f0, current.f1 + 1))
                .setParallelism(1).writeAsText("/results/prova.out");*/

        env.execute();

    }


    public static class MyMapper implements MapFunction<FriendshipEvent, Tuple3<Integer,Long,Long>> {

        @Override
        public Tuple3<Integer, Long,Long> map(FriendshipEvent event) throws Exception {
            return new Tuple3<>(event.getTimeSlot(),event.getUserId1(),event.getUserId2());
        }
    }

    public static class MyMapper2 implements MapFunction<Tuple3<Integer,Long,Long>,Tuple2<Integer,Long>>  {

        @Override
        public Tuple2<Integer, Long> map(Tuple3<Integer, Long, Long> event) throws Exception {
            return new Tuple2<>(event.f0,1L);
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


