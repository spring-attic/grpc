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
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.app.grpc.test.support.ProcessorServer;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author David Turanski
 **/
@SpringBootTest(classes = GrpcProcessorTests.TestConfiguration.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@RunWith(SpringRunner.class)
public abstract class GrpcProcessorTests {
	private static ProcessorServer server;
	private static ManagedChannel inProcessChannel;
	private static String serverName = UUID.randomUUID().toString();

	@Autowired
	private Environment environment;

	@BeforeClass
	public static void setUp() throws Exception {

		server = new ProcessorServer(InProcessServerBuilder.forName(serverName).directExecutor());
		server.start();
		inProcessChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		inProcessChannel.shutdownNow();
		server.stop();
	}

	public static class ProcessorTests extends GrpcProcessorTests {

		@Autowired
		private MessageCollector messageCollector;

		@Autowired
		private Processor processor;

		@Test
		public void test() throws InterruptedException {
			Message<?> request = MessageBuilder.withPayload("hello".getBytes())
				.copyHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE, "application/octet-stream")).build();
			processor.input().send(request);
			Message<?> message = messageCollector.forChannel(processor.output()).poll(2, TimeUnit.SECONDS);
			//TODO : The expected response is "HELLO".getBytes().  For convenience, MessageCollector converts it to
			// String, a real binder will not do that.
			assertThat(message.getPayload()).isEqualTo("HELLO");
			assertThat(message.getHeaders().getId()).isNotNull();
			assertThat(message.getHeaders().getTimestamp()).isNotZero();
		}
	}

	@TestPropertySource(properties = { "grpc.include-headers=true" })
	public static class ProcessorWithHeadersTests extends GrpcProcessorTests {

		@Autowired
		private MessageCollector messageCollector;

		@Autowired
		private Processor processor;

		@Autowired
		private GrpcProperties properties;

		@Test
		public void test() throws InterruptedException {

			assertThat(properties.isIncludeHeaders()).isTrue();
			doTest(messageCollector, processor);

		}
	}

	@TestPropertySource(properties = { "grpc.stub=async" })
	public static class AsyncProcessorTests extends GrpcProcessorTests {

		@Autowired
		private MessageCollector messageCollector;

		@Autowired
		private Processor processor;

		@Test
		public void test() throws InterruptedException {
			doTest(messageCollector, processor);
		}
	}

	protected void doTest(MessageCollector messageCollector, Processor processor) throws InterruptedException {
		Map<String, Object> headers = new HashMap<>();
		headers.put("int", 123);
		headers.put("str", "string");
		headers.put("pi", 3.14);
		processor.input().send(MessageBuilder.withPayload("hello".getBytes()).copyHeaders(headers).build());
		Message<?> message = messageCollector.forChannel(processor.output()).poll(2, TimeUnit.SECONDS);
		//TODO : The expected response is "HELLO".getBytes().  For convenience, MessageCollector converts it to
		// String, a real binder will not do that.
		assertThat(message.getPayload()).isEqualTo("HELLO");
		assertThat(message.getHeaders().getId()).isNotNull();
		assertThat(message.getHeaders().getTimestamp()).isNotZero();
		if (environment.getProperty("grpc.include-headers", "false").equals("true")) {
			assertThat(message.getHeaders().get("int")).isEqualTo(123);
			assertThat(message.getHeaders().get("str")).isEqualTo("string");
			assertThat(message.getHeaders().get("pi")).isEqualTo(3.14);
		}
	}

	@Configuration
	@EnableAutoConfiguration
	@Import(GrpcProcessorConfiguration.class)
	static class TestConfiguration {
		@Bean
		public Channel channel() {
			return inProcessChannel;
		}
	}
}

