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
import org.springframework.cloud.stream.app.grpc.message.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author David Turanski
 **/
public class ProtobufMessageBuilderTests {

	@Test
	public void testStringPayload() {
		org.springframework.messaging.Message<String> source = MessageBuilder.withPayload("hello")
				.copyHeaders(Collections.singletonMap("foo", "bar")).build();
		Message target = new ProtobufMessageBuilder().fromMessage(source).build();
		checkPayloadsAndHeadersEqual(source, target);
	}

	@Test
	public void testByteArrayPayload() {
		org.springframework.messaging.Message<byte[]> source = MessageBuilder.withPayload("hello".getBytes())
				.build();
		Message target = new ProtobufMessageBuilder().fromMessage(source).build();
		checkPayloadsAndHeadersEqual(source, target);
	}


	@Test
	public void testDoublePayload() {
		org.springframework.messaging.Message<Double> source = MessageBuilder.withPayload(4.0)
				.build();
		Message target = new ProtobufMessageBuilder().fromMessage(source).build();
		checkPayloadsAndHeadersEqual(source, target);
	}

	@Test
	public void testFloatPayload() {
		org.springframework.messaging.Message<Float> source = MessageBuilder.withPayload(4.0F)
				.build();
		Message target = new ProtobufMessageBuilder().fromMessage(source).build();
		checkPayloadsAndHeadersEqual(source, target);
	}

	@Test
	public void testIntPayload() {
		org.springframework.messaging.Message<Integer> source = MessageBuilder.withPayload(4)
				.build();
		Message target = new ProtobufMessageBuilder().fromMessage(source).build();
		checkPayloadsAndHeadersEqual(source, target);
	}

	@Test
	public void testLongPayload() {
		org.springframework.messaging.Message<Long> source = MessageBuilder.withPayload(4L)
				.build();
		Message target = new ProtobufMessageBuilder().fromMessage(source).build();
		checkPayloadsAndHeadersEqual(source, target);
	}

	@Test
	public void testBooleanPayload() {
		org.springframework.messaging.Message<Boolean> source = MessageBuilder.withPayload(true)
				.build();
		Message target = new ProtobufMessageBuilder().fromMessage(source).build();
		checkPayloadsAndHeadersEqual(source, target);
	}

	private void checkPayloadsAndHeadersEqual(org.springframework.messaging.Message<?> expected, Message target){
		org.springframework.messaging.Message<?> actual = MessageUtils.toMessage(target);
		assertThat(actual.getPayload()).isEqualTo(expected.getPayload());
		assertThat(actual.getHeaders()).isEqualTo(expected.getHeaders());
	}
}
