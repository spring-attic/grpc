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

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.app.grpc.support.MessageUtils;
import org.springframework.cloud.stream.app.grpc.support.ProtobufMessageBuilder;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;

/**
 * @author David Turanski
 **/

@Configuration
@Import(GrpcChannelConfiguration.class)
public class GrpcProcessorConfiguration {

	@Autowired
	private Channel grpcChannel;

	@Bean
	public ProcessorGrpc.ProcessorStub processorStub() {
		return ProcessorGrpc.newStub(grpcChannel);
	}

	@EnableBinding(Processor.class)
	static class ProcessorConfiguration {

		@Autowired
		private Processor channels;

		@Autowired
		private ProcessorGrpc.ProcessorStub processorStub;

		@StreamListener(Processor.INPUT)
		public void process(final Message<?> request) {

			org.springframework.cloud.stream.app.grpc.message.Message protobufMessage = new ProtobufMessageBuilder()
					.fromMessage(request).build();

			processorStub.process(protobufMessage,
					new StreamObserver<org.springframework.cloud.stream.app.grpc.message.Message>() {

						@Override
						public void onNext(org.springframework.cloud.stream.app.grpc.message.Message message) {
							channels.output().send(MessageUtils.toMessage(message));
						}

						@Override
						public void onError(Throwable throwable) {
							throw new MessagingException(request, throwable);
						}

						@Override
						public void onCompleted() {

						}
					});
		}
	}
}
