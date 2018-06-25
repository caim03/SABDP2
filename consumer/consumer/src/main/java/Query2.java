import events.CommentEvent;
import events.CommentTimestampExtractor;
import operators.apply.PostCounter;
import operators.apply.Ranking;
import operators.filter.PostFilter;
import operators.keyBy.KeyByPostId;
import operators.keyBy.KeyByWindowStart;
import operators.mapper.CommentMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

public class Query2 extends FlinkRabbitmq{
    public Query2(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting Rabbitmq Stream Processor..");

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

        DataStream<CommentEvent> dataStream = env.addSource(new RMQSource<String>(connectionConfig,
                commentQueue,
                new SimpleStringSchema())).map(line -> new CommentEvent(line));

        DataStream<String> filtered = dataStream
                .assignTimestampsAndWatermarks(new CommentTimestampExtractor())
                .map(new CommentMapper())
                .filter(new PostFilter())
                .keyBy(new KeyByPostId())
                .timeWindow(Time.days(7))
                .apply(new PostCounter())
                .keyBy(new KeyByWindowStart())
                .timeWindow(Time.days(7))
                .apply(new Ranking());

        filtered.writeAsText("/results/query2/prova.out").setParallelism(1);

        env.execute();
    }
}
