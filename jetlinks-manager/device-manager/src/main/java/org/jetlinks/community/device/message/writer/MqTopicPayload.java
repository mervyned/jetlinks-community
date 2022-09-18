package org.jetlinks.community.device.message.writer;

import lombok.Getter;
import lombok.Setter;

public class MqTopicPayload {
    @Getter
    @Setter
    private String topic;

    @Getter
    @Setter
    private String payload;

    public MqTopicPayload(String topic, String payload) {
        this.topic = topic;
        this.payload = payload;
    }
}
