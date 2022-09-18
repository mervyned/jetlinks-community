package org.jetlinks.community.device.message.writer;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.jetlinks.community.gateway.DeviceMessageUtils;
import org.jetlinks.core.message.*;
import org.jetlinks.core.message.event.EventMessage;
import org.jetlinks.core.message.property.ReportPropertyMessage;

import java.util.Map;
import java.util.Optional;

public class MqMessage {

    private String vendorTopic = "/smarthome/abox";

    public MqTopicPayload parseMessage(DeviceMessage message){

        String payload = "";
        String topic = "";

        // 设备属性上报
        if (message instanceof ReportPropertyMessage) {
            topic = getGatewayTopic(message);
            payload = tryGetPropertiesJson(message);
        }
        // 设备事件上报
        else if (message instanceof EventMessage) {
            topic = getEventTopic(message, null);
            payload = getEventJson(message, null);
        }
        // 设备上线
        else if (message instanceof DeviceOnlineMessage) {
            Object objFrom = message.getHeaders().getOrDefault("from", "");
            if (!String.valueOf(objFrom).equals("session-register")) {
                topic = getStatusTopic(message, null);
                payload = getStatusJson(message, null);
            }
        }
        // 设备离线
        else if (message instanceof DeviceOfflineMessage) {
            Object objFrom = message.getHeaders().getOrDefault("from", "");
            if (!String.valueOf(objFrom).equals("session-unregister")) {
                topic = getStatusTopic(message, null);
                payload = getStatusJson(message, null);
            }
        }
        // 子设备消息上报
        else if (message instanceof ChildDeviceMessage) {
            Message msg = ((ChildDeviceMessage) message).getChildDeviceMessage();
            // 子设备属性
            if (msg instanceof ReportPropertyMessage) {
                ReportPropertyMessage childMessage = (ReportPropertyMessage) msg;
                topic = getDeviceTopic(message, childMessage);
                payload = tryGetPropertiesJson(childMessage);
            }
            // 子设备事件
            else if (msg instanceof EventMessage) {
                EventMessage childMessage = (EventMessage) msg;
                topic = getEventTopic(message, childMessage);
                payload = getEventJson(message, childMessage);
            }
            // 子设备上线
            else if (msg instanceof DeviceOnlineMessage) {
                Object objFrom = message.getHeaders().getOrDefault("from", "");
                if (!String.valueOf(objFrom).equals("session-register")) {
                    DeviceOnlineMessage childMessage = (DeviceOnlineMessage) msg;
                    topic = getStatusTopic(message, childMessage);
                    payload = getStatusJson(message, childMessage);
                }
            }
            // 子设备离线
            else if (msg instanceof DeviceOfflineMessage) {
                Object objFrom = message.getHeaders().getOrDefault("from", "");
                if (!String.valueOf(objFrom).equals("session-unregister")) {
                    DeviceOfflineMessage childMessage = (DeviceOfflineMessage) msg;
                    topic = getStatusTopic(message, childMessage);
                    payload = getStatusJson(message, childMessage);
                }
            }
        }

        if (StringUtils.isNotEmpty(topic) & StringUtils.isNotEmpty(payload)) {
            return new MqTopicPayload(topic, payload);
        }
        return null;
    };

    private String tryGetPropertiesJson(DeviceMessage message) {
        Optional<Map<String, Object>> opt = DeviceMessageUtils.tryGetProperties(message);
        if (opt.isPresent()) {
            JSONObject jsonObject = new JSONObject(opt.get());
            return jsonObject.toJSONString();
        }
        return "";
    }

    private String getGatewayTopic(DeviceMessage message) {
        String deviceId = message.getDeviceId();
        String productId = message.getHeaderOrDefault(Headers.productId);
        if (StringUtils.isNotEmpty(productId)) {
            return String.format("%s/%s/%s/properties/report", vendorTopic, productId, deviceId);
        }
        return "";
    }

    private String getDeviceTopic(DeviceMessage gatewayMessage, DeviceMessage deviceMessage) {
        String gatewayId = gatewayMessage.getDeviceId();
        String productId = gatewayMessage.getHeaderOrDefault(Headers.productId);
        if (StringUtils.isNotEmpty(productId)) {
            return String.format("%s/%s/%s/child/%s/properties/report", vendorTopic, productId, gatewayId, deviceMessage.getDeviceId());
        }
        return "";
    }

    private String getEventTopic(DeviceMessage gatewayMessage, DeviceMessage deviceMessage) {
        String gatewayId = gatewayMessage.getDeviceId();
        String productId = gatewayMessage.getHeaderOrDefault(Headers.productId);
        if (StringUtils.isNotEmpty(productId)) {
            if (deviceMessage == null) {
                if (gatewayMessage instanceof EventMessage) {
                    String event = ((EventMessage) gatewayMessage).getEvent();
                    return String.format("/smarthome/abox/%s/%s/event/%s", productId, gatewayId, event);
                }
            } else {
                if (deviceMessage instanceof EventMessage) {
                    String event = ((EventMessage) deviceMessage).getEvent();
                    return String.format("/smarthome/abox/%s/%s/child/%s/event/%s", productId, gatewayId, deviceMessage.getDeviceId(), event);
                }
            }
        }
        return "";
    }

    private String getEventJson(DeviceMessage gatewayMessage, DeviceMessage deviceMessage) {
        EventMessage msg = null;
        if (deviceMessage != null) {
            if (deviceMessage instanceof EventMessage) {
                msg = (EventMessage) deviceMessage;
            }
        } else {
            if (gatewayMessage instanceof EventMessage) {
                msg = (EventMessage) gatewayMessage;
            }
        }
        if (msg != null) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("event", msg.getEvent());
            //jsonObject.put("messageId", msg.getMessageId());
            jsonObject.put("timestamp", msg.getTimestamp());
            jsonObject.put("data", msg.getData());
            return jsonObject.toJSONString();
        }
        return "";
    }

    /**
     * 设备在线离线topic
     *
     * @param gatewayMessage
     * @param deviceMessage
     * @return
     */
    private String getStatusTopic(DeviceMessage gatewayMessage, DeviceMessage deviceMessage) {
        String gatewayId = gatewayMessage.getDeviceId();
        String productId = gatewayMessage.getHeaderOrDefault(Headers.productId);
        if (StringUtils.isNotEmpty(productId)) {
            if (deviceMessage == null) {
                String status = "";
                if (gatewayMessage instanceof DeviceOnlineMessage) {
                    status = "online";
                } else if (gatewayMessage instanceof DeviceOfflineMessage) {
                    status = "offline";
                }
                return String.format("/smarthome/abox/%s/%s/%s", productId, gatewayId, status);

            } else {
                String status = "";
                if (deviceMessage instanceof DeviceOnlineMessage) {
                    status = "online";
                } else if (deviceMessage instanceof DeviceOfflineMessage) {
                    status = "offline";
                }
                return String.format("/smarthome/abox/%s/%s/child/%s/%s", productId, gatewayId, deviceMessage.getDeviceId(), status);
            }
        }
        return "";
    }

    /**
     * 设备在线离线json
     *
     * @param gatewayMessage
     * @param deviceMessage
     * @return
     */
    private String getStatusJson(DeviceMessage gatewayMessage, DeviceMessage deviceMessage) {
        DeviceMessage msg = deviceMessage == null ? gatewayMessage : deviceMessage;
        if (msg != null) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("deviceId", msg.getDeviceId());
            //jsonObject.put("messageId", msg.getMessageId());
            jsonObject.put("timestamp", msg.getTimestamp());
            return jsonObject.toJSONString();
        }
        return "";
    }
}
