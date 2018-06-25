package events;

import java.text.SimpleDateFormat;

/**
 * Created by Caim03 on 20/06/18.
 */
public class FriendshipEvent extends Event {
    private long userId1;
    private long userId2;
    private int timeSlot;

    public FriendshipEvent(String record, boolean b) {
        super(record.split("\\|")[0]);

        String[] arrayList = record.split("\\|");

        Long user1 = Long.valueOf(arrayList[1]);
        Long user2 = Long.valueOf(arrayList[2]);

        if(b){
            if(user1<user2) {
                this.userId1 = user1;
                this.userId2 = user2;
            }
            else
            {
                this.userId1 = user2;
                this.userId1 = user1;
            }
        }
        else{
            this.userId1 = user1;
            this.userId2 = user2;
        }



        this.timeSlot = Integer.valueOf(arrayList[0].substring(11,13));
    }

    public long getUserId1() {
        return userId1;
    }

    public void setUserId1(long userId1) {
        this.userId1 = userId1;
    }

    public long getUserId2() {
        return userId2;
    }

    public void setUserId2(long userId2) {
        this.userId2 = userId2;
    }

    public int getTimeSlot() {
        return timeSlot;
    }

    public void setTimeSlot(int timeSlot) {
        this.timeSlot = timeSlot;
    }

    @Override
    public String toString() {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss\"");
        String time = sdf.format(timestamp);

        return "FriendshipEvent{" +
                " timestamp=" + time +
                ", userId1=" + userId1 +
                ", userId2=" + userId2 +
                ", hour=" + timeSlot +
                '}';
    }
}
