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

import function.Function;
import org.springframework.cloud.stream.app.grpc.processor.ProcessorProtos.HeaderValue;
import org.springframework.cloud.stream.app.grpc.processor.ProcessorProtos.Message;
import org.springframework.integration.support.MutableMessageBuilder;
import org.springframework.messaging.MessageHeaders;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author David Turanski
 **/
public abstract class MessageUtils {
	public static org.springframework.messaging.Message<byte[]> toMessage(Message message) {

		Map<String, Object> headers = new LinkedHashMap<>();
		for (Map.Entry<String, HeaderValue> header : message.getHeadersMap().entrySet()) {
			if (header.getKey().equals(MessageHeaders.ID)) {
				headers.put(header.getKey(), UUID.fromString(header.getValue().getValues(0)));
			}
			else if (header.getKey().equals(MessageHeaders.TIMESTAMP)) {
				headers.put(header.getKey(), Long.valueOf(header.getValue().getValues(0)));
			}
			else {
				if (!header.getValue().getValuesList().isEmpty()) {
					if (header.getValue().getValuesList().size() == 1) {
						headers.put(header.getKey(), header.getValue().getValues(0));
					}
					else {
						headers.put(header.getKey(), header.getValue().getValuesList());
					}
				}
			}
		}

		return MutableMessageBuilder.withPayload(message.getPayload().toByteArray()).copyHeaders(headers).build();
	}

	public static org.springframework.messaging.Message<byte[]> toMessage(Function.Message message) {

		Map<String, Object> headers = new LinkedHashMap<>();
		for (Map.Entry<String, Function.Message.HeaderValue> header : message.getHeadersMap().entrySet()) {
			if (header.getKey().equals(MessageHeaders.ID)) {
				headers.put(header.getKey(), UUID.fromString(header.getValue().getValues(0)));
			}
			else if (header.getKey().equals(MessageHeaders.TIMESTAMP)) {
				headers.put(header.getKey(), Long.valueOf(header.getValue().getValues(0)));
			}
			else {
				if (!header.getValue().getValuesList().isEmpty()) {
					if (header.getValue().getValuesList().size() == 1) {
						headers.put(header.getKey(), header.getValue().getValues(0));
					}
					else {
						headers.put(header.getKey(), header.getValue().getValuesList());
					}
				}
			}
		}

		return MutableMessageBuilder.withPayload(message.getPayload().toByteArray()).copyHeaders(headers).build();
	}

}
