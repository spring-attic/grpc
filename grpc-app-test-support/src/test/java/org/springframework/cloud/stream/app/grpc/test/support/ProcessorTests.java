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

package org.springframework.cloud.stream.app.grpc.test.support;

/**
 * @author David Turanski
 **/

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.cloud.stream.app.grpc.processor.Message;
import org.springframework.cloud.stream.app.grpc.processor.ProcessorGrpc;
import org.springframework.cloud.stream.app.grpc.support.ProtobufMessageBuilder;
import org.springframework.messaging.support.GenericMessage;

/**
 * Unit tests for {@link ProcessorServer}.
 */

public class ProcessorTests {
	private ProcessorServer server;
	private ManagedChannel inProcessChannel;

	@Before
	public void setUp() throws Exception {
		String uniqueServerName = "in-process server for " + getClass();

		server = new ProcessorServer(InProcessServerBuilder.forName(uniqueServerName).directExecutor());
		server.start();
		inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor().build();
	}

	@After
	public void tearDown() throws Exception {
		inProcessChannel.shutdownNow();
		server.stop();
	}

	@Test
	public void process() {

		Message message = new ProtobufMessageBuilder().fromMessage(new GenericMessage<>("hello, world".getBytes())).build();

		ProcessorGrpc.ProcessorBlockingStub stub = ProcessorGrpc.newBlockingStub(inProcessChannel);

		Message response = stub.process(message);


		Assert.assertEquals("HELLO, WORLD", response.getPayload().toStringUtf8());
	}

}