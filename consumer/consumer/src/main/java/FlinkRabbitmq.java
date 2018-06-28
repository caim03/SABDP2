import com.rabbitmq.client.AMQP;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class FlinkRabbitmq extends RMQSource {

    public static String exchangeName          = "simpl_exchange";
    public static String friendQueue           = "friendship";
    public static String postQueue             = "posts";
    public static String query1_24h            = "query1_24h";
    public static String query1_7d             = "query1_7d";
    public static String query1_4ever          = "query1_4ever";
    public static String query2_1h             = "query2_1h";
    public static String query2_1d             = "query2_1d";
    public static String query2_1w             = "query2_1w";
    public static String query3_1h             = "query3_1h";
    public static String query3_1d             = "query3_1d";
    public static String query3_1w             = "query3_1w";
    public static String commentQueue          = "comments";
    public static String rabbitmqHostname      = "rabbitmq";
    public static String rabbitmqVirtualHost   = "/";
    public static String rabbitmqUsername      = "rabbitmq";
    public static String rabbitmqPassword      = "rabbitmq";
    public static Integer rabbitmqPort         = 5672;
    public static boolean durableQueue         = false;

    public static Logger logger = LoggerFactory.getLogger(Query1.class);

    public FlinkRabbitmq(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    @Override
    protected void setupQueue() throws IOException {
        AMQP.Queue.DeclareOk result = channel.queueDeclare(queueName, true, durableQueue, false, null);
        channel.queueBind(result.getQueue(), exchangeName, "*");
    }
}