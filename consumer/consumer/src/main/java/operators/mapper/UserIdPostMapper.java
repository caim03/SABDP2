package operators.mapper;

import events.PostEvent;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by Caim03 on 25/06/18.
 */
public class UserIdPostMapper implements MapFunction<PostEvent, Long> {
    @Override
    public Long map(PostEvent postEvent) throws Exception {
        return postEvent.getUserId();
    }
}
