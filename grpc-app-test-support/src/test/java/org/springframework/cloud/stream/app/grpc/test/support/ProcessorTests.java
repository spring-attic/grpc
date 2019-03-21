/*
 * Copyright 2018 the original author or authors.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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

import static org.assertj.core.api.Fail.fail;
import static org.assertj.core.api.Java6Assertions.assertThat;

import io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.cloud.stream.app.grpc.processor.ProcessorGrpc;
import org.springframework.cloud.stream.app.grpc.processor.ProcessorProtos;
import org.springframework.cloud.stream.app.grpc.processor.ProcessorProtos.Message;
import org.springframework.cloud.stream.app.grpc.processor.ReactorProcessorGrpc;
import org.springframework.cloud.stream.app.grpc.support.ProtobufMessageBuilder;
import org.springframework.messaging.support.GenericMessage;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.LinkedList;
import java.util.List;

/**
 * Unit tests for {@link ProcessorServer}.
 */

public class ProcessorTests extends AbstractProcessorTest {


	@BeforeClass
	public static void setup() throws Exception {
		init(new ProcessorServer());
	}

	@Test
	public void process() {

		Message message = new ProtobufMessageBuilder().fromMessage(new GenericMessage<>("hello, world".getBytes()))
			.build();

		ProcessorGrpc.ProcessorBlockingStub stub = ProcessorGrpc.newBlockingStub(getChannel());

		Message response = stub.process(message);

		Assert.assertEquals("HELLO, WORLD", response.getPayload().toStringUtf8());
	}

	@Test
	public void stream() {

		StreamObserver<Message> responseObserver = new StreamObserver<Message>() {
			List<String> responses = new LinkedList<>();

			@Override
			public void onNext(Message message) {
				responses.add(message.getPayload().toStringUtf8());
			}

			@Override
			public void onError(Throwable throwable) {
				fail(throwable.getMessage());
			}

			@Override
			public void onCompleted() {
				assertThat(responses).containsExactly("APPLE", "BANANA");

			}
		};
		ProcessorGrpc.ProcessorStub stub = ProcessorGrpc.newStub(getChannel());

		StreamObserver<Message> requestObserver = stub.stream(responseObserver);

		requestObserver.onNext(
			new ProtobufMessageBuilder().fromMessage(new GenericMessage<>("apple".getBytes())).build());
		requestObserver.onNext(
			new ProtobufMessageBuilder().fromMessage(new GenericMessage<>("banana".getBytes())).build());
		requestObserver.onCompleted();
	}

	@Test
	public void reactorStubToGrpcStreamServer() throws InterruptedException {

		ReactorProcessorGrpc.ReactorProcessorStub stub = ReactorProcessorGrpc.newReactorStub(getChannel());

		Flux<ProcessorProtos.Message> input = Flux.just(
			new ProtobufMessageBuilder().fromMessage(new GenericMessage<>("apple".getBytes())).build(),
			new ProtobufMessageBuilder().fromMessage(new GenericMessage<>("banana".getBytes())).build());

		Flux<ProcessorProtos.Message> output = stub.stream(input);

		StepVerifier.create(output.map(m -> m.getPayload().toStringUtf8()))
			.expectNext("APPLE")
			.expectNext("BANANA")
			.expectComplete()
			.verify();
	}

}