package org.jetlinks.community.device.message.writer;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.gateway.annotation.Subscribe;
import org.jetlinks.core.message.DeviceMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderResult;

/**
 * 用于将设备消息写入到时序数据库
 *
 * @author zhouhao
 * @since 1.0
 */
@Slf4j
@Component
public class MqKafkaWriterConnector {

//    @Autowired
//    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ReactiveKafkaProducerTemplate template;

    public MqKafkaWriterConnector() {
    }

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
            String topic = mqTopicPayload.getTopic().substring(1);
            Mono<SenderResult<Void>> resultMono = template.send(topic.replace("/", "-"), mqTopicPayload.getPayload());
            return resultMono.flatMap(rs -> {
                if(rs.exception() != null) {
                    System.out.println("send kafka error" + rs.exception());
                    return Mono.empty();
                }
                return Mono.empty();
            });
        }
        return null;
    }
}