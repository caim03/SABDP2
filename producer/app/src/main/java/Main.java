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
        while(i<friendsData.size())
        {
            friendManager.send(friendsData.get(i));
            i++;
        }
//
//        for(int i=0; i < 100; i++){
//            friendManager.send(friendsData.get(i));
//        }
        /*postManager.send(postsData.get(0));
        commentManager.send(commentsData.get(0));*/

        /* Una volta finito chiudo */
        friendManager.terminate();
        postManager.terminate();
        commentManager.terminate();
    }
}

