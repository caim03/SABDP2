import events.FriendshipEvent;
import events.FriendshipTimestampExtractor;
import operators.apply.Counter;
import operators.apply.StringConcat;
import operators.evictor.UserEvictor;
import operators.keyBy.MyKey;
import operators.keyBy.MyKey2;
import operators.keyBy.WindowKey;
import operators.mapper.MyMapper;
import operators.mapper.MyMapper2;
import operators.mapper.MyMapper3;
import operators.reducer.CountReducer;
import operators.reducer.ReduceDuplicate;
import operators.trigger.UserTrigger;
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
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

public class Query1 extends FlinkRabbitmq {

    public Query1(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting Rabbitmq Stream Processor..");
        /* METTERE QUESTO PARAMETRO A TRUE SOLO SE SI FA GLOBAL STREAM */
        boolean allStreamFriend = true;

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
                //Assegna il watermarks (è una sorta di campo nascosto in cui lui controlla il tempo che scorre nel flusso)
                .assignTimestampsAndWatermarks(new FriendshipTimestampExtractor())
                //Map to (TS,id1,id2)
                .map(new MyMapper())
                //Tutto come chiave
                .<KeyedStream<Tuple3<Integer, Long, Long>,Tuple3<Integer, Long, Long>>>keyBy(0,1,2);


        if(!allStreamFriend){
            SingleOutputStreamOperator hoursStream = commonStream
                    //Elimina i duplicati nell'ultimo giorno (per fare la reduce serve inserirli in una finestra, chissà se si può aumentare sta finestra?)
                    .timeWindow(Time.days(1))
                    .reduce(new ReduceDuplicate())
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
                    .apply(new StringConcat());


            SingleOutputStreamOperator weekStream = commonStream
                    //Elimina i duplicati nell'ultimo giorno (per fare la reduce serve inserirli in una finestra, chissà se si può aumentare sta finestra?)
                    .timeWindow(Time.days(7))
                    .reduce(new ReduceDuplicate())
                    //Mappa in TS e basta
                    .map(new MyMapper2()) //Mappa in Tuple3<Timeslot,id1,id2>
                /*Chiave = TS (per le timewindows bisogna sempre raggruppare per chiave, perchè praticamente è come se
                creasse una windows per ogni chiave*/
                    .keyBy(new MyKey())
                    .timeWindow(Time.days(7))
                    //Conta le righe per ogni TS ed all'interno della finestra. Produce un: <InizioWindow,TS, somma)
                    .apply(new Counter())
                    //Key by inizio window (una sorta di groupby per raggruppare tutti gli inizi di finestra e printarli insieme)
                    .keyBy(new WindowKey())
                    .timeWindow(Time.days(7))
                    //Produce la stringa di output
                    .apply(new StringConcat());

            hoursStream.writeAsText("/results/query1/24hours.out").setParallelism(1);
            weekStream.writeAsText("/results/query1/7days.out").setParallelism(1);
        }

        else{
            SingleOutputStreamOperator allStream = commonStream
                    .reduce(new ReduceDuplicate())
                    .map(new MyMapper3())
                    .keyBy(new MyKey2())
                    .window(GlobalWindows.create())
                    .trigger(new UserTrigger())
                    .evictor(new UserEvictor())
                    .apply(new CountReducer());

            allStream.writeAsText("/results/query1/allDays.out").setParallelism(1);
        }

        env.execute();
    }
}


