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

package org.springframework.cloud.stream.app.grpc.processor;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Channel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.app.grpc.test.support.AbstractProcessorTest;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @author David Turanski
 **/

@SpringBootTest(classes = GrpcProcessorIntegrationTests.TestConfiguration.class,
	webEnvironment = SpringBootTest.WebEnvironment.NONE,
	properties = {"grpc.host=localhost", "grpc.port=10382", "grpc.stub=riff"})
@RunWith(SpringRunner.class)
public class GrpcProcessorIntegrationTests {

	@Autowired
	private MessageCollector messageCollector;

	@Autowired
	private Processor processor;

	@Test
	public void test() throws InterruptedException {
		Message<?> request = MessageBuilder.withPayload("{\"text\":\"happy\"}".getBytes())
			.copyHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE, "application/octet-stream"))
			.build();
		processor.input().send(request);
		Message<?> message = messageCollector.forChannel(processor.output()).poll(2, TimeUnit.SECONDS);
		assertThat(message).isNotNull();
		System.out.println(message);
	}

	@Configuration
	@EnableAutoConfiguration
	@Import(GrpcProcessorConfiguration.class)
	static class TestConfiguration {
		@Bean
		public MessageConverter grpcMessageConverter() {
			return new MessageConverter() {
				@Nullable
				@Override
				public Object fromMessage(Message<?> message, Class<?> targetClass) {
					System.out.println("fromMessage target " + targetClass);
					return null;
				}

				@Nullable
				@Override
				public Message<?> toMessage(Object payload, @Nullable MessageHeaders headers) {
					System.out.println("toMessage target " + payload);
					return null;
				}
			};
		}

	}
}

