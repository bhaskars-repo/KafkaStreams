/*
 * Name:   QtyEvent timestamp extractor
 * Author: Bhaskar S
 * Date:   12/10/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams;

import com.polarsparc.kstreams.model.QtyEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class QtyEventTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long prevTimestamp) {
        long ts = consumerRecord.timestamp();

        QtyEvent evt = (QtyEvent) consumerRecord.value();
        evt.setTimestamp(ts);

        return ts;
    }
}
