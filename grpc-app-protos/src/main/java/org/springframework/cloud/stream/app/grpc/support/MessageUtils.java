/*
 * Copyright 2017 the original author or authors.
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

import org.springframework.cloud.stream.app.grpc.processor.Generic;
import org.springframework.cloud.stream.app.grpc.processor.Message;
import org.springframework.integration.support.MutableMessageBuilder;
import org.springframework.messaging.MessageHeaders;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author David Turanski
 **/
public abstract class MessageUtils {
	public static org.springframework.messaging.Message<?> toMessage(Message message) {
		FromGenericConverter fromGenericConverter = new FromGenericConverter();
		Map<String, Object> headers = new LinkedHashMap<>();
		for (Map.Entry<String, Generic> header : message.getHeadersMap().entrySet()) {
			if (header.getKey().equals(MessageHeaders.ID)) {
				headers.put(header.getKey(), UUID.fromString(fromGenericConverter.convert(header.getValue()).toString()));
			}
			else {
				headers.put(header.getKey(), fromGenericConverter.convert(header.getValue()));
			}
		}

		return MutableMessageBuilder.withPayload(fromGenericConverter.convert(message.getPayload())).copyHeaders(headers)
				.build();
	}
}
