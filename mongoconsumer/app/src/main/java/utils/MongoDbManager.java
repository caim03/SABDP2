package utils;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class MongoDbManager {

    static MongoClient mongoClient;
    static MongoDatabase db;

    static MongoDbManager mongoDbManager = null;

    public MongoDbManager()
    {

        MongoClientURI uri = new MongoClientURI("mongodb://mongo:mongo@mongo:27017");

        mongoClient = new MongoClient(uri);

        db = mongoClient.getDatabase("sabdp2");

        db.getCollection("query1");
        db.getCollection("query2");
        db.getCollection("query3");

    }



    public static MongoDbManager getDb() {
        if(mongoDbManager==null)
        {
            mongoDbManager = new MongoDbManager();
        }
        return mongoDbManager;


    }
    public void save(String body, String queue)
    {
        Document doc = new Document();
        doc.append("Start" , body.split(",")[0]);
        doc.append("Values" , body.substring(body.indexOf(",")+2,body.length()));

        db.getCollection(queue).insertOne(doc);


    }




}
