package operators.mapper;

import events.FriendshipEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Map a stream of FriendshipEvent in Tuple3<Integer, Long, Long> (Time Slot, UserId1, UserId2)
 */
public class FriendshipInitMapper implements MapFunction<FriendshipEvent, Tuple3<Integer,Long,Long>> {

    @Override
    public Tuple3<Integer, Long,Long> map(FriendshipEvent event) throws Exception {
        return new Tuple3<>(event.getTimeSlot(),event.getUserId1(),event.getUserId2());
    }
}