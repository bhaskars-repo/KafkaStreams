/*
 * Name:   Common Utilities
 * Author: Bhaskar S
 * Date:   11/25/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public interface KafkaStreamsUtils {
    static ObjectMapper getObjectMapper() {
        final ObjectMapper mapper = new ObjectMapper();

        // Very important for handling LocalDateTime
        {
            mapper.registerModule(new JavaTimeModule());
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        }

        return mapper;
    }
}
