import events.FriendshipEvent;
import events.FriendshipTimestampExtractor;
import operators.aggregator.FriendShipCounterAgg;
import operators.aggregator.FullFriendshipCounterAgg;
import operators.aggregator.StringConcatAgg;
import operators.apply.FriendShipCounter;
import operators.apply.StringConcat;
import operators.evictor.UserEvictor;
import operators.keyBy.KeyByTimeSlot;
import operators.keyBy.MyKey2;
import operators.keyBy.WindowKey;
import operators.mapper.FriendshipInitMapper;
import operators.mapper.TimeSlotMapper;
import operators.mapper.MyMapper3;
import operators.apply.FullFriendshipCounter;
import operators.reducer.ReduceDuplicate;
import operators.trigger.UserTrigger;
import operators.windowFunction.FriendshipCounterWF;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.util.Properties;

public class Query1 extends FlinkRabbitmq {

    public Query1(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting Rabbitmq Stream Processor..");

        /**
         *  fullStream: T if windowed stream, F if global windowed (full stream analysis)
         *  writeOnFile: T write on shared file, F write on an apposite rabbitMQ queue
         *  useApply: T with apply operations, F with aggregates operations (faster)
         */
        boolean fullStream = false;
        Properties properties = new ReadProperties().getProperties();
        boolean writeOnFile = Boolean.parseBoolean(properties.getProperty("writeOnFile"));
        boolean useApply = Boolean.parseBoolean(properties.getProperty("useApply"));



        //Set output path
        Path path = new Path("/results/query1");

        if(FileSystem.getLocalFileSystem().exists(path)){
            FileSystem.getLocalFileSystem().delete(path, true);
        }

        //Set up rabbitMQ Connection config
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(rabbitmqHostname).setPort(rabbitmqPort).setUserName(rabbitmqUsername)
                .setPassword(rabbitmqPassword).setVirtualHost(rabbitmqVirtualHost)
                .build();

        //Set up environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //Initial mapping of the dataStream in FriendshipEvent
        //Small trick: userId1 and userId2 have been set in ascending order ( => bidirectional friendship are egual! ;) )
        DataStream<FriendshipEvent> dataStream = env.addSource(new RMQSource<>(connectionConfig,
                friendQueue,
                new SimpleStringSchema())).map(line -> new FriendshipEvent(line, true));


        /*----Initial Schema----
          .Assign an apposite watermark
          .Map a stream of FriendshipEvent in Tuple3<Integer, Long, Long> (Time Slot, UserId1, UserId2)
          .Take all as key (we need to filter duplicates next)
        */
        KeyedStream<Tuple3<Integer, Long, Long>, Tuple> commonStream = dataStream
                .assignTimestampsAndWatermarks(new FriendshipTimestampExtractor())
                .map(new FriendshipInitMapper())
                .<KeyedStream<Tuple3<Integer, Long, Long>,Tuple3<Integer, Long, Long>>>keyBy(0,1,2);


        /*----Query1 Schema----
           .timeWindow
           .reduce (just take one of the two inputs, for remove duplicates (= bidirectional friend requests)
           .map into a single Integer (Time Slot of current record)
           .key by that unique field
           .timeWindow
           .apply/aggregate: count each rows which have the same Time Slot and build a tuple3<Start time of the window, Timeslot, sum>
           .key by the first field (start time of the window)
           .timeWindow
           .apply/aggregate: build an output formatted string ("Start window , TS , sum ...")
         */
        if(!fullStream){

            DataStream<String> hoursStream;
            DataStream<String> weekStream;

            if(useApply)
            {
                hoursStream = commonStream
                        .timeWindow(Time.days(1))
                        .reduce(new ReduceDuplicate())
                        .map(new TimeSlotMapper())
                        .keyBy(new KeyByTimeSlot())
                        .timeWindow(Time.days(1))
                        .apply(new FriendShipCounter())
                        .keyBy(new WindowKey())
                        .timeWindow(Time.days(1))
                        .apply(new StringConcat());

                weekStream = commonStream
                        .timeWindow(Time.days(7))
                        .reduce(new ReduceDuplicate())
                        .map(new TimeSlotMapper())
                        .keyBy(new KeyByTimeSlot())
                        .timeWindow(Time.days(7))
                        .apply(new FriendShipCounter())
                        .keyBy(new WindowKey())
                        .timeWindow(Time.days(7))
                        .apply(new StringConcat());
            }
            else
            {
                hoursStream = commonStream
                        .timeWindow(Time.days(1))
                        .reduce(new ReduceDuplicate())
                        .map(new TimeSlotMapper())
                        .keyBy(new KeyByTimeSlot())
                        .timeWindow(Time.days(1))
                        .aggregate(new FriendShipCounterAgg(), new FriendshipCounterWF())
                        .keyBy(new WindowKey())
                        .timeWindow(Time.days(1))
                        .aggregate(new StringConcatAgg());

                weekStream = commonStream
                        .timeWindow(Time.days(7))
                        .reduce(new ReduceDuplicate())
                        .map(new TimeSlotMapper())
                        .keyBy(new KeyByTimeSlot())
                        .timeWindow(Time.days(7))
                        .aggregate(new FriendShipCounterAgg(), new FriendshipCounterWF())
                        .keyBy(new WindowKey())
                        .timeWindow(Time.days(7))
                        .aggregate(new StringConcatAgg());
            }

            if(writeOnFile)
            {
                hoursStream.writeAsText("/results/query1/24hours.out").setParallelism(1);
                weekStream.writeAsText("/results/query1/7days.out").setParallelism(1);

            }
            else {

                hoursStream.addSink(new RMQSink<String>(
                        connectionConfig,            // config for the RabbitMQ connection
                        query1_24h,                 // name of the RabbitMQ queue to send messages to
                        new SimpleStringSchema()));  // serialization schema to turn Java objects to messages

                weekStream.addSink(new RMQSink<String>(
                        connectionConfig,            // config for the RabbitMQ connection
                        query1_7d,                 // name of the RabbitMQ queue to send messages to
                        new SimpleStringSchema()));  // serialization schema to turn Java objects to messages
            }
        }

        else{
            SingleOutputStreamOperator allStream;
            if(useApply)
                allStream = commonStream
                        .reduce(new ReduceDuplicate())
                        .map(new MyMapper3())
                        .keyBy(new MyKey2())
                        .window(GlobalWindows.create())
                        .trigger(new UserTrigger())
                        .evictor(new UserEvictor())
                        .apply(new FullFriendshipCounter());

            else
                allStream = commonStream
                        .reduce(new ReduceDuplicate())
                        .map(new MyMapper3())
                        .keyBy(new MyKey2())
                        .window(GlobalWindows.create())
                        .trigger(new UserTrigger())
                        .evictor(new UserEvictor())
                        .aggregate(new FullFriendshipCounterAgg());

            if(writeOnFile)
                allStream.writeAsText("/results/query1/allDays.out").setParallelism(1);
            else
                allStream.addSink(new RMQSink<String>(
                        connectionConfig,            // config for the RabbitMQ connection
                        query1_4ever,                 // name of the RabbitMQ queue to send messages to
                        new SimpleStringSchema()));  // serialization schema to turn Java objects to messages
        }
        env.execute();
    }
}


