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

    public static void main(String[] args) throws java.io.IOException, TimeoutException, InterruptedException {
        ReadProperties readProperties = new ReadProperties();
        Properties properties = readProperties.getProperties();
        ReaderManager readerManager = ReaderManager.getInstance();
        ArrayList<String> friendsData;
        ArrayList<String> postsData;
        ArrayList<String> commentsData;

        /* RabbitMQ Config */
        final String HOST = properties.getProperty("host");
        final String USER = properties.getProperty("username");
        final String PWD = properties.getProperty("password");

        /* Queues */
        final String FRIENDS = properties.getProperty("friendship");
        final String POSTS = properties.getProperty("posts");
        final String COMMENTS = properties.getProperty("comments");

        /* Data Paths */
        final String FRIENDPATH = properties.getProperty("friendData");
        final String POSTPATH = properties.getProperty("postData");
        final String COMMENTPATH = properties.getProperty("commentData");

        friendsData = readerManager.readFile(FRIENDPATH);
        postsData = readerManager.readFile(POSTPATH);
        commentsData = readerManager.readFile(COMMENTPATH);

        RabbitMQManager friendManager = new RabbitMQManager(HOST, USER, PWD, FRIENDS);
        RabbitMQManager postManager = new RabbitMQManager(HOST, USER, PWD, POSTS);
        RabbitMQManager commentManager = new RabbitMQManager(HOST, USER, PWD, COMMENTS);

        int i = 0;
        long size = Math.max(Math.max(friendsData.size(), postsData.size()), commentsData.size());

        while(i<size)
        {
            if(i < friendsData.size()){
                friendManager.send(friendsData.get(i));
            }

            if(i < postsData.size()){
                postManager.send(postsData.get(i));
            }

            if(i < commentsData.size()){
                commentManager.send(commentsData.get(i));
            }
            i++;
        }

        /* Una volta finito chiudo */
        friendManager.terminate();
        postManager.terminate();
        commentManager.terminate();
    }
}

