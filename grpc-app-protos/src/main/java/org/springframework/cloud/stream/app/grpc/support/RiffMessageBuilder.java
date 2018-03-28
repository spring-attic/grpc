/*
 * Copyright 2018 the original author or authors.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.springframework.cloud.stream.app.grpc.support;

import com.google.protobuf.ByteString;
import function.Function;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author David Turanski
 **/
public class RiffMessageBuilder {

	private static Log logger = LogFactory.getLog(RiffMessageBuilder.class);

	private Map<String, Function.Message.HeaderValue> headers;
	private byte[] payload;
	private Function.Message.Builder builder = Function.Message.newBuilder();

	public RiffMessageBuilder() {
	}

	public RiffMessageBuilder withPayload(byte[] payload) {
		this.payload = payload;
		return this;
	}

	public RiffMessageBuilder withHeaders(MessageHeaders messageHeaders) {

		Map<String, Function.Message.HeaderValue> headers = messageHeaders.entrySet()
			.stream()
			.collect(Collectors.toMap(Map.Entry::getKey, e -> {
				Function.Message.HeaderValue.Builder builder = Function.Message.HeaderValue.newBuilder();

				if (e.getKey() == MessageHeaders.ID) {
					builder.addValues(e.getValue().toString());
				}
				else if (e.getKey() == MessageHeaders.TIMESTAMP) {
					builder.addValues(String.valueOf((long) e.getValue()));
				}

				else if (e.getValue() instanceof String) {
					builder.addValues((String) e.getValue());

				}

				else if (e.getValue() instanceof MimeType) {
					builder.addValues(e.getValue().toString());
				}

				else if (e.getValue() instanceof Iterable<?>) {
					try {
						builder.addAllValues((Iterable<String>) e.getValue());

					}
					catch (ClassCastException e1) {
						logger.warn(String.format("Header %s is not mapped to gRPC message. Unsupported element type",
							e.getKey()));
					}
				}
				else if (e.getValue().getClass().isArray()) {
					Object[] array = (Object[]) e.getValue();
					for (Object obj : array) {
						builder.addValues(obj.toString());
					}
				}
				else {
					if (e.getValue() != null) {
						logger.warn(
							String.format("Header %s is not mapped  to gRPC message. Unsupported type %s", e.getKey(),
								e.getValue().getClass().getName()));
					}
				}
				return builder.build();
			}));
		this.headers = headers;
		return this;
	}

	public RiffMessageBuilder withProtobufHeaders(Map<String, Function.Message.HeaderValue> messageHeaders) {
		headers = messageHeaders;
		return this;
	}

	public RiffMessageBuilder fromMessage(org.springframework.messaging.Message<?> message) {
		return this.withHeaders(message.getHeaders()).withPayload((byte[]) message.getPayload());
	}

	public Function.Message build() {
		Assert.notNull(this.payload, "payload cannot be null.");
		if (headers == null) {
			return builder.setPayload(ByteString.copyFrom(payload)).build();
		}

		return builder.putAllHeaders(headers).setPayload(ByteString.copyFrom(payload)).build();
	}
}
