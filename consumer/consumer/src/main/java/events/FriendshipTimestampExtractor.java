package events;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * Created by Caim03 on 20/06/18.
 */
public class FriendshipTimestampExtractor implements AssignerWithPeriodicWatermarks<FriendshipEvent> {
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis());
    }

    @Override
    public long extractTimestamp(FriendshipEvent e, long l) {
        return e.getTimestamp();
    }
}
