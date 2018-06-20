package events;

/**
 * Created by Caim03 on 20/06/18.
 */
public class FriendshipEvent extends Event {
    private long userId1;
    private long userId2;
    private int timeSlot;

    public FriendshipEvent(String record) {
        super(record.split("\\|")[0]);

        String[] arrayList = record.split("\\|");
        this.userId1 = Long.valueOf(arrayList[1]);
        this.userId2 = Long.valueOf(arrayList[2]);
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
        return "FriendshipEvent{" +
                "timestamp=" + timestamp +
                ", userId1=" + userId1 +
                ", userId2=" + userId2 +
                ", hour=" + timeSlot +
                '}';
    }
}
