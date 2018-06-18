/**
 * Created by Caim03 on 18/06/18.
 */

import utils.RabbitMQManager;
import utils.ReadProperties;
import utils.ReaderManager;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class Main {

    public static void main(String[] args) throws java.io.IOException, TimeoutException {
        ReadProperties readProperties = new ReadProperties();
        Properties properties = readProperties.getProperties();
        ReaderManager readerManager = ReaderManager.getInstance();
        ArrayList<String> data;

        data = readerManager.readFile("data/friendships.dat");

        RabbitMQManager manager = new RabbitMQManager(properties.getProperty("host"),
                properties.getProperty("username"),
                properties.getProperty("password"),
                properties.getProperty("queue"));

        /* Ne scrivo una sola per prova */
        String message = data.get(0);
        manager.send(message);

        /* Una volta finito chiudo */
        manager.terminate();
    }
}
