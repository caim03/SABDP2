import events.*;
import operators.apply.JoinCounter;
import operators.apply.Ranking;
import operators.apply.UserCounter;
import operators.keyBy.JoinKey;
import operators.keyBy.KeyByUser;
import operators.keyBy.KeyByWindowStart;
import operators.mapper.UserIdCommentMapper;
import operators.mapper.UserIdFriendMapper;
import operators.mapper.UserIdPostMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

public class Query3 extends FlinkRabbitmq{

    public Query3(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting Rabbitmq Stream Processor..");

        Path path = new Path("/results/query3");

        if(FileSystem.getLocalFileSystem().exists(path)){
            FileSystem.getLocalFileSystem().delete(path, true);
        }

        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(rabbitmqHostname).setPort(rabbitmqPort).setUserName(rabbitmqUsername)
                .setPassword(rabbitmqPassword).setVirtualHost(rabbitmqVirtualHost)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<FriendshipEvent> friendStream = env.addSource(new RMQSource<String>(connectionConfig,
                friendQueue,
                new SimpleStringSchema())).map(line -> new FriendshipEvent(line, false));

        DataStream<PostEvent> postStream = env.addSource(new RMQSource<String>(connectionConfig,
                postQueue,
                new SimpleStringSchema())).map(line -> new PostEvent(line));

        DataStream<CommentEvent> commentStream = env.addSource(new RMQSource<String>(connectionConfig,
                commentQueue,
                new SimpleStringSchema())).map(line -> new CommentEvent(line));



        DataStream<Tuple3<Long, Long, Long>> friendQuery = friendStream
                .assignTimestampsAndWatermarks(new FriendshipTimestampExtractor())
                .map(new UserIdFriendMapper())
                .keyBy(new KeyByUser())
                .timeWindow(Time.days(1))
                .apply(new UserCounter());

        DataStream<Tuple3<Long, Long, Long>> postQuery = postStream
                .assignTimestampsAndWatermarks(new PostTimestampExtractor())
                .map(new UserIdPostMapper())
                .keyBy(new KeyByUser())
                .timeWindow(Time.days(1))
                .apply(new UserCounter());

        DataStream<Tuple3<Long, Long, Long>> commentQuery = commentStream
                .assignTimestampsAndWatermarks(new CommentTimestampExtractor())
                .map(new UserIdCommentMapper())
                .keyBy(new KeyByUser())
                .timeWindow(Time.days(1))
                .apply(new UserCounter());

        DataStream<String> unionStream = friendQuery
                .union(postQuery, commentQuery)
                .keyBy(new JoinKey())
                .timeWindow(Time.days(1))
                .apply(new JoinCounter())
                .keyBy(new KeyByWindowStart())
                .timeWindow(Time.days(1))
                .apply(new Ranking());

        unionStream.writeAsText("/results/query3/prova.out").setParallelism(1);

        env.execute();
    }
}
