package events;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * Created by Caim03 on 20/06/18.
 */
public class CommentTimestampExtractor extends AscendingTimestampExtractor<CommentEvent> {

    @Override
    public long extractAscendingTimestamp(CommentEvent commentEvent) {
        return commentEvent.getTimestamp();
    }
}