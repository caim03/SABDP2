/**
 * Created by Caim03 on 18/06/18.
 */

import utils.RabbitMQManager;

import java.util.concurrent.TimeoutException;

public class Main {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws java.io.IOException, TimeoutException {
        RabbitMQManager manager = new RabbitMQManager("rabbitmq", "rabbitmq", "rabbitmq", "queue");

        int i = 0;
        while(i < 10){
            manager.send("Message: " + i);
            i++;
        }
    }
}
