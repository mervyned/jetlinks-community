package org.jetlinks.community.device.message.writer;

import com.alibaba.fastjson.JSONObject;
import io.vertx.core.Vertx;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.jetlinks.community.gateway.DeviceMessageUtils;
import org.jetlinks.community.gateway.annotation.Subscribe;
import org.jetlinks.community.network.mqtt.client.VertxMqttClient;
import org.jetlinks.core.message.*;
import org.jetlinks.core.message.codec.MqttMessage;
import org.jetlinks.core.message.codec.SimpleMqttMessage;
import org.jetlinks.core.message.event.EventMessage;
import org.jetlinks.core.message.property.ReportPropertyMessage;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.awt.*;
import java.util.Map;
import java.util.Optional;

/**
 * 用于将设备消息写入到时序数据库
 *
 * @author zhouhao
 * @since 1.0
 */
@Slf4j
@Component
public class MqRabbitMqWriterConnector {

    private VertxMqttClient mqttClient;

    /**
     * 订阅设备消息
     *
     * @param message 设备消息
     * @return void
     */
    @Subscribe(topics = "/device/**", id = "device-message-ts-writer")
    public Mono<Void> writeMessage(DeviceMessage message) {

        MqMessage mqMessage = new MqMessage();
        MqTopicPayload mqTopicPayload = mqMessage.parseMessage(message);

        if (mqTopicPayload != null) {
            MqttMessage mqttMessage = SimpleMqttMessage
                .builder()
                .topic(mqTopicPayload.getTopic())
                .payload(mqTopicPayload.getPayload())
                .build();
            return mqttClient
                .publish(mqttMessage)
                .then();
        }
        return null;
    }


    @PostConstruct
    private void initMqttClient() {
        Vertx vertx = Vertx.vertx();
        MqttClientOptions options = new MqttClientOptions()
            .setUsername("emqx").setPassword("weeu3789suy")
            .setWillFlag(true).setWillRetain(true).setWillQoS(1).setWillTopic("oneths").setWillMessage("ONETHS_IOT");
        MqttClient client = MqttClient.create(vertx, options);
        client.connect(1884, "iot-core.mervyn.com.cn", result -> {
            if (!result.succeeded()) {
                log.warn("connect mqtt [amqp] error");
            } else {
                log.debug("connect mqtt [amqp] success");
            }
        });
        mqttClient = new VertxMqttClient("test123");
        mqttClient.setClient(client);
    }
}