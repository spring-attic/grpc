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
import static org.junit.Assert.fail;

import com.google.protobuf.Empty;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.app.grpc.test.support.ProcessorServer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

/**
 * @author David Turanski
 **/
@SpringBootTest(classes = PingTests.TestConfiguration.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@RunWith(SpringRunner.class)
public class PingTests {
	private static ProcessorServer server;
	private static ManagedChannel inProcessChannel;
	private static String serverName = UUID.randomUUID().toString();

	@Autowired
	@Qualifier("sideCarHealthIndicator")
	private HealthIndicator sideCarHealthIndicator;

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

	@Autowired
	private ProcessorGrpc.ProcessorBlockingStub pingStub;

	@Test
	public void ping() {

		for (int i = 0; i < 2; i++) {
			ProcessorProtos.Status status = pingStub.ping(Empty.getDefaultInstance());
			assertThat(status.getMessage()).isEqualTo("alive");
		}

		try {
			pingStub.ping(Empty.getDefaultInstance());
			fail("Should throw exception");
		}
		catch (StatusRuntimeException e) {
			assertThat(e.getStatus()).isEqualTo(io.grpc.Status.UNKNOWN);
		}
	}

	@Test
	public void healthIndicator() {
		assertThat(sideCarHealthIndicator.health().getStatus().getCode()).isEqualTo("alive");
		assertThat(sideCarHealthIndicator.health().getStatus().getCode()).isEqualTo("alive");
		assertThat(sideCarHealthIndicator.health().getStatus().getCode()).isEqualTo("DOWN");
	}

	@Configuration
	@EnableAutoConfiguration
	@Import({ GrpcProcessorConfiguration.class })
	static class TestConfiguration {
		@Bean
		public Channel channel() {
			return inProcessChannel;
		}
	}

}
