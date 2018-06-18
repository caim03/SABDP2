package utils;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

public class ReadProperties {

    public Properties getProperties(){
        InputStream inputStream;
        ArrayList<String> properties = new ArrayList<>();

        Properties prop = new Properties();
        String propFileName = "config.properties";

        try{
            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if(inputStream != null){
                prop.load(inputStream);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return prop;
    }
}
