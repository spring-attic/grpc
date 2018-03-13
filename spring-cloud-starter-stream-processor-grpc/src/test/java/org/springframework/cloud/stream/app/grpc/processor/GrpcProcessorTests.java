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
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.app.grpc.test.support.ProcessorServer;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author David Turanski
 **/
@SpringBootTest(classes = {GrpcProcessorTests.TestConfiguration.class},
	webEnvironment = SpringBootTest.WebEnvironment
	.NONE)
@RunWith(SpringRunner.class)
public abstract class GrpcProcessorTests {
	private static ProcessorServer server;
	private static ManagedChannel inProcessChannel;
	private static String serverName = UUID.randomUUID().toString();


	@SpringBootApplication
	@Import({TestChannelBinderConfiguration.class, GrpcProcessorConfiguration.class})
	static class TestConfiguration {
		@Bean
		public Channel channel() {
			return inProcessChannel;
		}
	}

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
		private InputDestination input;

		@Autowired
		private OutputDestination output;

		@Test
		public void test() throws InterruptedException {
			input.send(new GenericMessage<>("hello".getBytes()));
			Message<byte[]> response = output.receive(1);

			assertThat(response.getPayload()).isEqualTo("HELLO".getBytes());
			assertThat(response.getHeaders().getId()).isNotNull();
			assertThat(response.getHeaders().getTimestamp()).isNotZero();
		}
	}

	@TestPropertySource(properties = { "grpc.include-headers=true" })
	public static class ProcessorWithHeadersTests extends GrpcProcessorTests {

		@Autowired
		private InputDestination input;

		@Autowired
		private OutputDestination output;

		@Test
		public void test() throws InterruptedException {
			doTest(input, output);
		}
	}

	@TestPropertySource(
		properties = { "grpc.stub=async" ,
		"logging.level.org.springframework.cloud.stream.binding=DEBUG"
	})
	public static class AsyncProcessorTests extends GrpcProcessorTests {

		@Autowired
		private InputDestination input;

		@Autowired
		private OutputDestination output;

		@Test
		public void test() throws InterruptedException {
			doTest(input, output);
		}
	}

	protected void doTest(InputDestination input, OutputDestination output) throws InterruptedException {
		Map<String, Object> headers = new HashMap<>();
		headers.put("int", 123);
		headers.put("str", "string");
		headers.put("pi", 3.14);
		headers.put(HttpHeaders.ACCEPT, Stream.of("application/json","text/plain").toArray());
		headers.put("string_list", Stream.of("application/json","text/plain").collect(Collectors.toList()));
		headers.put("int_list", Stream.of(1,2,3).collect(Collectors.toList()));
		headers.put(MessageHeaders.CONTENT_TYPE, "application/octet-stream");
		headers.put("some_ints", Stream.of(1,2).toArray());

		Message<byte[]> request = MessageBuilder.withPayload("hello".getBytes()).copyHeaders(headers).build();

		input.send(request);
		Message<byte[]> response = output.receive(1);

		assertThat(response.getPayload()).isEqualTo("HELLO".getBytes());
		assertThat(response.getHeaders().getId()).isNotNull();
		assertThat(response.getHeaders().getTimestamp()).isNotZero();

		if (environment.getProperty("grpc.include-headers", "false").equals("true")) {
			assertThat(response.getHeaders().get("int")).isEqualTo(123);
			assertThat(response.getHeaders().get("str")).isEqualTo("string");
			assertThat(response.getHeaders().get("pi")).isEqualTo(3.14);
			assertThat(response.getHeaders().get(HttpHeaders.ACCEPT)).asList().containsExactly("application/json","text/plain");
			assertThat(response.getHeaders().get(MessageHeaders.CONTENT_TYPE)).isEqualTo("application/octet-stream");
		}
	}


}

