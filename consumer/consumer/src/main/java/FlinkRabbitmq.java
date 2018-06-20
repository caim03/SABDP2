import com.rabbitmq.client.AMQP;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class FlinkRabbitmq extends RMQSource {

    public static String exchangeName          = "simpl_exchange";
    public static String queueName             = "friendship";
    public static String rabbitmqHostname      = "rabbitmq";
    public static String rabbitmqVirtualHost   = "/";
    public static String rabbitmqUsername      = "rabbitmq";
    public static String rabbitmqPassword      = "rabbitmq";
    public static Integer rabbitmqPort         = 5672;
    public static boolean durableQueue         = true;

    public static Logger logger = LoggerFactory.getLogger(RabbitmqStreamProcessor.class);

    public FlinkRabbitmq(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    @Override
    protected void setupQueue() throws IOException {
        AMQP.Queue.DeclareOk result = channel.queueDeclare(queueName, false, durableQueue, false, null);
        channel.queueBind(result.getQueue(), exchangeName, "*");
    }
}