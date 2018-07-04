import events.CommentEvent;
import events.CommentTimestampExtractor;
import operators.aggregator.PostCounterAgg;
import operators.apply.PostCounter;
import operators.apply.Ranking;
import operators.filter.PostFilter;
import operators.keyBy.KeyByPostId;
import operators.keyBy.KeyByWindowStart;
import operators.mapper.CommentMapper;
import operators.windowFunction.ProcessPostCounterWF;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.util.Properties;

public class Query2 extends FlinkRabbitmq{
    public Query2(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting Rabbitmq Stream Processor..");

        /**
         *  writeOnFile: T write on shared file, F write on an apposite rabbitMQ queue
         *  useApply: T with apply operations, F with aggregates operations (faster)
         */
        Properties properties = new ReadProperties().getProperties();
        boolean writeOnFile = Boolean.parseBoolean(properties.getProperty("writeOnFile"));
        boolean useApply = Boolean.parseBoolean(properties.getProperty("useApply"));

        //Set output path
        Path path = new Path("/results/query2");

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
        //Small trick: we will set commentReplied as -1 if the comment is a reply and not a direct comment
        DataStream<CommentEvent> dataStream = env.addSource(new RMQSource<>(connectionConfig,
                commentQueue,
                new SimpleStringSchema())).map(line -> new CommentEvent(line));

        /*----Initial Schema----
          .Assign an apposite watermark
          .Map a stream of CommentEvent in Tuple2<Long,Long> (postCommented, commentReplied)
          .filter by commentReplied != -1 (we want only direct comments)
          .keyBy the postId
        */
        KeyedStream<Tuple2<Long, Long>, Long> filtered = dataStream
                .assignTimestampsAndWatermarks(new CommentTimestampExtractor())
                .map(new CommentMapper())
                .filter(new PostFilter())
                .keyBy(new KeyByPostId());

        DataStream<String> hourStream;
        DataStream<String> dayStream;
        DataStream<String> weekStream;

        /*----Query Schema----
        .timeWindow
        .apply/aggregate: count each row with the same post id and build a tuple3<Start time of the window, postid, sum>
        .key by the first field (start time of the window)
        .timeWindow
        .apply: ranking of the first 10 most commented post of each window, and build an output formatted string ("startWindow  , postId , sum ...") ;
         */
        if(useApply)
        {
            hourStream = filtered
                    .timeWindow(Time.hours(1))
                    .apply(new PostCounter())
                    .keyBy(new KeyByWindowStart())
                    .timeWindow(Time.hours(1))
                    .apply(new Ranking());

            dayStream = filtered
                    .timeWindow(Time.days(1))
                    .apply(new PostCounter())
                    .keyBy(new KeyByWindowStart())
                    .timeWindow(Time.days(1))
                    .apply(new Ranking());

            weekStream = filtered
                    .timeWindow(Time.days(7))
                    .apply(new PostCounter())
                    .keyBy(new KeyByWindowStart())
                    .timeWindow(Time.days(7))
                    .apply(new Ranking());
        }
        else
        {
            hourStream = filtered
                    .timeWindow(Time.hours(1))
                    .aggregate(new PostCounterAgg(), new ProcessPostCounterWF())
                    .keyBy(new KeyByWindowStart())
                    .timeWindow(Time.hours(1))
                    .apply(new Ranking());

            dayStream = filtered
                    .timeWindow(Time.days(1))
                    .aggregate(new PostCounterAgg(), new ProcessPostCounterWF())
                    .keyBy(new KeyByWindowStart())
                    .timeWindow(Time.days(1))
                    .apply(new Ranking());

            weekStream = filtered
                    .timeWindow(Time.days(7))
                    .aggregate(new PostCounterAgg(), new ProcessPostCounterWF())
                    .keyBy(new KeyByWindowStart())
                    .timeWindow(Time.days(7))
                    .apply(new Ranking());
        }

        if(writeOnFile) {
            hourStream.writeAsText("/results/query2/1hour.out").setParallelism(1);
            dayStream.writeAsText("/results/query2/1day.out").setParallelism(1);
            weekStream.writeAsText("/results/query2/1week.out").setParallelism(1);
        }
        else
        {
            hourStream.addSink(new RMQSink<String>(
                    connectionConfig,            // config for the RabbitMQ connection
                    query2_1h,                 // name of the RabbitMQ queue to send messages to
                    new SimpleStringSchema()));  // serialization schema to turn Java objects to messages

            dayStream.addSink(new RMQSink<String>(
                    connectionConfig,            // config for the RabbitMQ connection
                    query2_1d,                 // name of the RabbitMQ queue to send messages to
                    new SimpleStringSchema()));  // serialization schema to turn Java objects to messages

            weekStream.addSink(new RMQSink<String>(
                    connectionConfig,            // config for the RabbitMQ connection
                    query2_1w,                 // name of the RabbitMQ queue to send messages to
                    new SimpleStringSchema()));  // serialization schema to turn Java objects to messages

        }

        env.execute();
    }
}
