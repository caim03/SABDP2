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

        DataStream<FriendshipEvent> friendStream = env.addSource(new RMQSource<>(connectionConfig,
                friendQueue,
                new SimpleStringSchema())).map(line -> new FriendshipEvent(line, false));

        DataStream<PostEvent> postStream = env.addSource(new RMQSource<>(connectionConfig,
                postQueue,
                new SimpleStringSchema())).map(line -> new PostEvent(line));

        DataStream<CommentEvent> commentStream = env.addSource(new RMQSource<>(connectionConfig,
                commentQueue,
                new SimpleStringSchema())).map(line -> new CommentEvent(line));


        DataStream<String> unionStreamHour = streaming(0, friendStream, postStream, commentStream);
        DataStream<String> unionStreamDay = streaming(1, friendStream, postStream, commentStream);
        DataStream<String> unionStreamWeek = streaming(2, friendStream, postStream, commentStream);


        unionStreamHour.writeAsText("/results/query3/1hour.out").setParallelism(1);
        unionStreamDay.writeAsText("/results/query3/1day.out").setParallelism(1);
        unionStreamWeek.writeAsText("/results/query3/1week.out").setParallelism(1);

        env.execute();
    }

    private static DataStream<String> streaming(int type, DataStream<FriendshipEvent> friendStream,
                                                DataStream<PostEvent> postStream, DataStream<CommentEvent> commentStream){

        Time timeWindow;

        switch (type){
            case 0:
                timeWindow = Time.hours(1);
                break;
            case 1:
                timeWindow = Time.days(1);
                break;
            case 2:
                timeWindow = Time.days(7);
                break;
            default:
                timeWindow = Time.hours(1);
        }

        DataStream<Tuple3<Long, Long, Long>> friendQuery = friendStream
                .assignTimestampsAndWatermarks(new FriendshipTimestampExtractor())
                .map(new UserIdFriendMapper())
                .keyBy(new KeyByUser())
                .timeWindow(timeWindow)
                .apply(new UserCounter());

        DataStream<Tuple3<Long, Long, Long>> postQuery = postStream
                .assignTimestampsAndWatermarks(new PostTimestampExtractor())
                .map(new UserIdPostMapper())
                .keyBy(new KeyByUser())
                .timeWindow(timeWindow)
                .apply(new UserCounter());

        DataStream<Tuple3<Long, Long, Long>> commentQuery = commentStream
                .assignTimestampsAndWatermarks(new CommentTimestampExtractor())
                .map(new UserIdCommentMapper())
                .keyBy(new KeyByUser())
                .timeWindow(timeWindow)
                .apply(new UserCounter());

        DataStream<String> unionStream = friendQuery
                .union(postQuery, commentQuery)
                .keyBy(new JoinKey())
                .timeWindow(timeWindow)
                .apply(new JoinCounter())
                .keyBy(new KeyByWindowStart())
                .timeWindow(timeWindow)
                .apply(new Ranking());

        return unionStream;
    }
}
