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
        final String Q1 = properties.getProperty("query1");
        final String Q2 = properties.getProperty("query2");
        final String Q3 = properties.getProperty("query3");


        RabbitMQManager query1Manager = new RabbitMQManager(HOST, USER, PWD, Q1);
        RabbitMQManager query2Manager = new RabbitMQManager(HOST, USER, PWD, Q2);
        RabbitMQManager query3Manager = new RabbitMQManager(HOST, USER, PWD, Q3);

        MongoDbManager.getDb();

        query1Manager.createDetachedReader();
//        query2Manager.createDetachedReader();
//        query3Manager.createDetachedReader();


        /* Una volta finito chiudo */
//        query1Manager.terminate();
//        query2Manager.terminate();
//        query3Manager.terminate();
    }
}

