import events.CommentEvent;
import events.CommentTimestampExtractor;
import operators.apply.PostCounter;
import operators.apply.Ranking;
import operators.filter.PostFilter;
import operators.keyBy.KeyByPostId;
import operators.keyBy.KeyByWindowStart;
import operators.mapper.CommentMapper;
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

public class Query2 extends FlinkRabbitmq{
    public Query2(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting Rabbitmq Stream Processor..");

        boolean writeOnFile = false;

        Path path = new Path("/results/query2");

        if(FileSystem.getLocalFileSystem().exists(path)){
            FileSystem.getLocalFileSystem().delete(path, true);
        }

        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(rabbitmqHostname).setPort(rabbitmqPort).setUserName(rabbitmqUsername)
                .setPassword(rabbitmqPassword).setVirtualHost(rabbitmqVirtualHost)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<CommentEvent> dataStream = env.addSource(new RMQSource<>(connectionConfig,
                commentQueue,
                new SimpleStringSchema())).map(line -> new CommentEvent(line));

        KeyedStream<Tuple2<Long, Long>, Long> filtered = dataStream
                .assignTimestampsAndWatermarks(new CommentTimestampExtractor())
                .map(new CommentMapper())
                .filter(new PostFilter())
                .keyBy(new KeyByPostId());

        DataStream<String> hourStream = filtered
                .timeWindow(Time.hours(1))
                .apply(new PostCounter())
                .keyBy(new KeyByWindowStart())
                .timeWindow(Time.hours(1))
                .apply(new Ranking());

        DataStream<String> dayStream = filtered
                .timeWindow(Time.days(1))
                .apply(new PostCounter())
                .keyBy(new KeyByWindowStart())
                .timeWindow(Time.days(1))
                .apply(new Ranking());

        DataStream<String> weekStream = filtered
                .timeWindow(Time.days(7))
                .apply(new PostCounter())
                .keyBy(new KeyByWindowStart())
                .timeWindow(Time.days(7))
                .apply(new Ranking());


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
