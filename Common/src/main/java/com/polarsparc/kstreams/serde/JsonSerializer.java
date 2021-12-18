/*
 * Name:   JSON Serializer
 * Author: Bhaskar S
 * Date:   11/25/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.polarsparc.kstreams.KafkaStreamsUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper mapper = KafkaStreamsUtils.getObjectMapper();

    @Override
    public byte[] serialize(String s, T data) {
        if (data == null) {
            return null;
        }
        try {
            return mapper.writeValueAsBytes(data);
        }
        catch (Exception ex) {
            throw new SerializationException(ex.getMessage());
        }
    }
}
