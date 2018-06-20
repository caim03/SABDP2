package events;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;


/**
 * Created by Caim03 on 20/06/18.
 */
public class PostTimestampExtractor implements AssignerWithPeriodicWatermarks<PostEvent> {
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis());
    }

    @Override
    public long extractTimestamp(PostEvent e, long l) {
        return e.getTimestamp();
    }
}