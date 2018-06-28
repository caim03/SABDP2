package operators.mapper;

import events.FriendshipEvent;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by Caim03 on 25/06/18.
 */
public class UserIdFriendMapper implements MapFunction<FriendshipEvent, Long> {
    @Override
    public Long map(FriendshipEvent friendshipEvent) throws Exception {
        return friendshipEvent.getUserId1();
    }
}
