package events;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Caim03 on 20/06/18.
 */
public abstract class Event {
    protected long timestamp;

    public Event(String time){
        try{
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            Date dt = sdf.parse(time);
            this.timestamp = dt.getTime();
        }
        catch (ParseException e){
            e.printStackTrace();
        }

    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
