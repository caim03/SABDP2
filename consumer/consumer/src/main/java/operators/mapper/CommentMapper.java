package operators.mapper;

import events.CommentEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by Caim03 on 25/06/18.
 */
public class CommentMapper implements MapFunction<CommentEvent, Tuple2<Long, Long>> {
    @Override
    public Tuple2<Long, Long> map(CommentEvent commentEvent) throws Exception {
        return new Tuple2<>(commentEvent.getPostCommented(), commentEvent.getCommentReplied());
    }
}
