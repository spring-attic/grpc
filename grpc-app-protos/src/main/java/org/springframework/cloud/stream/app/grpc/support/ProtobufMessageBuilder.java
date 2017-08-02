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

import org.springframework.cloud.stream.app.grpc.message.Generic;
import org.springframework.cloud.stream.app.grpc.message.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;

/**
 * @author David Turanski
 **/
public class ProtobufMessageBuilder {

	private ProtobufMessageHeaders headers;
	private Generic payload;
	private Message.Builder builder = Message.newBuilder();
	private ToGenericConverter toGenericConverter = new ToGenericConverter();

	public ProtobufMessageBuilder() {
	}

	public ProtobufMessageBuilder withPayload(Object payload) {
		this.payload = toGenericConverter.convert(payload);
		return this;
	}

	public ProtobufMessageBuilder withHeaders(MessageHeaders messageHeaders) {
		headers = new ProtobufMessageHeaders(messageHeaders);
		return this;
	}

	public ProtobufMessageBuilder withHeaders(ProtobufMessageHeaders messageHeaders) {
		headers = messageHeaders;
		return this;
	}

	public ProtobufMessageBuilder fromMessage(org.springframework.messaging.Message<?> message) {
		this.withHeaders(message.getHeaders());
		return this.withPayload(message.getPayload());
	}

	public Message build() {
		Assert.notNull(this.payload, "payload cannot be null.");
		Assert.notEmpty(this.headers, "headers cannot be empty or null.");
		return builder.putAllHeaders(this.headers.asMap()).setPayload(this.payload).build();
	}

}
