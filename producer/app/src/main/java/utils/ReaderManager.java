package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Caim03 on 18/06/18.
 */
public class ReaderManager {
    private static ReaderManager readerManager = null;

    private ReaderManager(){

    }

    public static ReaderManager getInstance(){
        if(readerManager == null){
            readerManager = new ReaderManager();
        }
        return readerManager;
    }

    public ArrayList<String> readFile(String path) throws IOException {
        BufferedReader bufferedReader = null;
        String line;
        ArrayList<String> lines = new ArrayList<>();

        try {
            bufferedReader = new BufferedReader(new FileReader(path));
            line = bufferedReader.readLine();
            while (line != null) {
                //String[] data = line.split("\\|");
                lines.add(line);
                line = bufferedReader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }

        return lines;
    }
}
