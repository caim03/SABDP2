package operators.mapper;

import events.CommentEvent;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by Caim03 on 25/06/18.
 */
public class UserIdCommentMapper implements MapFunction<CommentEvent, Long> {
    @Override
    public Long map(CommentEvent commentEvent) throws Exception {
        return commentEvent.getUserId();
    }
}
