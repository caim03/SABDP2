/**
 * Created by Caim03 on 18/06/18.
 */

import utils.MongoDbManager;
import utils.RabbitMQManager;
import utils.ReadProperties;
import utils.ReaderManager;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class Main {

    public static void main(String[] args) throws java.io.IOException, TimeoutException, InterruptedException {
        ReadProperties readProperties = new ReadProperties();
        Properties properties = readProperties.getProperties();


        /* RabbitMQ Config */
        final String HOST = properties.getProperty("host");
        final String USER = properties.getProperty("username");
        final String PWD = properties.getProperty("password");

        /* Queues */
        final String Q1_24h = properties.getProperty("query1_24h");
        final String Q1_7d = properties.getProperty("query1_7d");
        final String Q1_4ever = properties.getProperty("query1_4ever");
        final String Q2_1h = properties.getProperty("query2_1h");
        final String Q2_1d = properties.getProperty("query2_1d");
        final String Q2_1w = properties.getProperty("query2_1w");
        final String Q3_1h = properties.getProperty("query3_1h");
        final String Q3_1d = properties.getProperty("query3_1d");
        final String Q3_1w = properties.getProperty("query3_1w");


        RabbitMQManager query1Manager_24h = new RabbitMQManager(HOST, USER, PWD, Q1_24h);
        RabbitMQManager query1Manager_7d = new RabbitMQManager(HOST, USER, PWD, Q1_7d);
        RabbitMQManager query1Manager_4ever = new RabbitMQManager(HOST, USER, PWD, Q1_4ever);
        RabbitMQManager query2Manager_1h = new RabbitMQManager(HOST, USER, PWD, Q2_1h);
        RabbitMQManager query2Manager_1d = new RabbitMQManager(HOST, USER, PWD, Q2_1d);
        RabbitMQManager query2Manager_1w = new RabbitMQManager(HOST, USER, PWD, Q2_1w);
        RabbitMQManager query3Manager_1h = new RabbitMQManager(HOST, USER, PWD, Q3_1h);
        RabbitMQManager query3Manager_1d = new RabbitMQManager(HOST, USER, PWD, Q3_1d);
        RabbitMQManager query3Manager_1w = new RabbitMQManager(HOST, USER, PWD, Q3_1w);

        MongoDbManager.getDb();

        query1Manager_24h.createDetachedReader();
        query1Manager_7d.createDetachedReader();

        query2Manager_1d.createDetachedReader();
        query2Manager_1h.createDetachedReader();
        query2Manager_1w.createDetachedReader();

        query3Manager_1d.createDetachedReader();
        query3Manager_1h.createDetachedReader();
        query3Manager_1w.createDetachedReader();

        /* Una volta finito chiudo */
//        query1Manager.terminate();
//        query2Manager.terminate();
//        query3Manager.terminate();
    }
}

