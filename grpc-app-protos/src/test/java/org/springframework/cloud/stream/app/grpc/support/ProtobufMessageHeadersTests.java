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

import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author David Turanski
 **/
public class ProtobufMessageHeadersTests {
	@Test
	public void testBasicHeaders() {

		org.springframework.messaging.Message<String> source = MessageBuilder.withPayload("hello")
				.copyHeaders(Collections.singletonMap("foo", "bar")).build();

		ProtobufMessageHeaders headers = new ProtobufMessageHeaders(source.getHeaders());

		assertThat(headers.get(MessageHeaders.ID)).isEqualTo(source.getHeaders().getId().toString());
		assertThat(headers.get("foo")).isEqualTo(source.getHeaders().get("foo"));
		assertThat(headers.get(MessageHeaders.TIMESTAMP)).isEqualTo(source.getHeaders().get(MessageHeaders.TIMESTAMP));
	}

	@Test
	public void testConvertToString() {
		org.springframework.messaging.Message<String> source = MessageBuilder.withPayload("hello")
				.copyHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)).build();

		ProtobufMessageHeaders headers = new ProtobufMessageHeaders(source.getHeaders());
		assertThat(headers.get(MessageHeaders.CONTENT_TYPE)).isEqualTo("application/json");
	}
}


