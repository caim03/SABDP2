import events.FriendshipEvent;
import events.FriendshipTimestampExtractor;
import operators.aggregator.FriendShipCounterAgg;
import operators.aggregator.FullFriendshipCounterAgg;
import operators.aggregator.StringConcatAgg;
import operators.apply.FriendShipCounter;
import operators.apply.StringConcat;
import operators.evictor.UserEvictor;
import operators.keyBy.MyKey;
import operators.keyBy.MyKey2;
import operators.keyBy.WindowKey;
import operators.mapper.MyMapper;
import operators.mapper.MyMapper2;
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

public class Query1 extends FlinkRabbitmq {

    public Query1(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting Rabbitmq Stream Processor..");

        boolean fullStream = false;
        boolean writeOnFile = true;
        boolean useApply = true;

        Path path = new Path("/results/query1");

        if(FileSystem.getLocalFileSystem().exists(path)){
            FileSystem.getLocalFileSystem().delete(path, true);
        }

        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(rabbitmqHostname).setPort(rabbitmqPort).setUserName(rabbitmqUsername)
                .setPassword(rabbitmqPassword).setVirtualHost(rabbitmqVirtualHost)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<FriendshipEvent> dataStream = env.addSource(new RMQSource<>(connectionConfig,
                friendQueue,
                new SimpleStringSchema())).map(line -> new FriendshipEvent(line, true));


        KeyedStream<Tuple3<Integer, Long, Long>, Tuple> commonStream = dataStream
                .assignTimestampsAndWatermarks(new FriendshipTimestampExtractor())
                .map(new MyMapper())
                .<KeyedStream<Tuple3<Integer, Long, Long>,Tuple3<Integer, Long, Long>>>keyBy(0,1,2);



        if(!fullStream){

            DataStream<String> hoursStream;
            DataStream<String> weekStream;

            if(useApply)
            {
                hoursStream = commonStream
                        .timeWindow(Time.days(1))
                        .reduce(new ReduceDuplicate())
                        .map(new MyMapper2())
                        .keyBy(new MyKey())
                        .timeWindow(Time.days(1))
                        .apply(new FriendShipCounter())
                        .keyBy(new WindowKey())
                        .timeWindow(Time.days(1))
                        .apply(new StringConcat());

                weekStream = commonStream
                        .timeWindow(Time.days(7))
                        .reduce(new ReduceDuplicate())
                        .map(new MyMapper2())
                        .keyBy(new MyKey())
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
                        .map(new MyMapper2())
                        .keyBy(new MyKey())
                        .timeWindow(Time.days(1))
                        .aggregate(new FriendShipCounterAgg(), new FriendshipCounterWF())
                        .keyBy(new WindowKey())
                        .timeWindow(Time.days(1))
                        .aggregate(new StringConcatAgg());

                weekStream = commonStream
                        .timeWindow(Time.days(7))
                        .reduce(new ReduceDuplicate())
                        .map(new MyMapper2())
                        .keyBy(new MyKey())
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
            //TODO else
        }

        env.execute();
    }
}


