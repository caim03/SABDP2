package events;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * Created by Caim03 on 20/06/18.
 */
public class FriendshipTimestampExtractor extends AscendingTimestampExtractor<FriendshipEvent> {
    @Override
    public long extractAscendingTimestamp(FriendshipEvent event) {
        return event.getTimestamp();
    }
}
