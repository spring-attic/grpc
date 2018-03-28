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

package org.springframework.cloud.stream.app.grpc.test.support;

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.cloud.stream.app.grpc.processor.ProcessorProtos;
import org.springframework.cloud.stream.app.grpc.processor.ReactorProcessorGrpc;
import org.springframework.cloud.stream.app.grpc.support.ProtobufMessageBuilder;
import org.springframework.messaging.support.GenericMessage;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * @author David Turanski
 **/
public class ReactorProcessorTests extends AbstractProcessorTest {


	@BeforeClass
	public static void setup() throws Exception {
		init(new ReactorProcessorServer());
	}

	@Test
	public void reactiveStreamToReactorServer() throws InterruptedException {

		ReactorProcessorGrpc.ReactorProcessorStub stub = ReactorProcessorGrpc.newReactorStub(getChannel());
		ProtobufMessageBuilder protobufMessageBuilder = new ProtobufMessageBuilder();

		Flux<ProcessorProtos.Message> input = Flux.just(
			protobufMessageBuilder.fromMessage(new GenericMessage<>("apple".getBytes())).build(),
			protobufMessageBuilder.fromMessage(new GenericMessage<>("banana".getBytes())).build());

		Flux<ProcessorProtos.Message> output = stub.stream(input);

		StepVerifier.create(output.map(m -> m.getPayload().toStringUtf8()))
			.expectNext("APPLE")
			.expectNext("BANANA")
			.expectComplete()
			.verify();

	}

}
