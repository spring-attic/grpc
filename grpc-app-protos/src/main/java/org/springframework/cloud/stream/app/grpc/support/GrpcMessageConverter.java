package org.springframework.cloud.stream.app.grpc.support;

import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

/**
 * @author David Turanski
 **/
public class GrpcMessageConverter implements MessageConverter {
	private ProtobufMessageBuilder protobufMessageBuilder = new ProtobufMessageBuilder();

	@Nullable
	@Override
	public Object fromMessage(Message<?> message, Class<?> targetClass) {
		return protobufMessageBuilder.fromMessage(message);
	}

	@Nullable
	@Override
	public Message<?> toMessage(Object payload, @Nullable MessageHeaders headers) {
		Assert.isInstanceOf(byte[].class, payload,
			String.format("Unsupported payload type [%s]. Expecting byte[]", payload.getClass().getName()));
		return MessageBuilder.withPayload((byte[]) payload).copyHeaders(headers).build();
	}
}
