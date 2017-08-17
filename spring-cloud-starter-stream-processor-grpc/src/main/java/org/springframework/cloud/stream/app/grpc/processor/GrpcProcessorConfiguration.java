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

import com.google.protobuf.Empty;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.app.grpc.support.MessageUtils;
import org.springframework.cloud.stream.app.grpc.support.ProtobufMessageBuilder;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;

import java.util.concurrent.TimeUnit;

/**
 * @author David Turanski
 **/

@EnableBinding(Processor.class)
@EnableConfigurationProperties(GrpcProperties.class)
public class GrpcProcessorConfiguration {

	@Autowired
	private Channel grpcChannel;

	@Autowired
	private GrpcProperties properties;

	@Autowired
	private Processor channels;

	@Autowired
	private ProcessorGrpc.ProcessorStub processorStub;

	@Bean
	public ProcessorGrpc.ProcessorStub processorStub() {
		return ProcessorGrpc.newStub(grpcChannel);
	}

	@Bean
	@ConditionalOnProperty(name = "grpc.host")
	public Channel grpcChannel(GrpcProperties properties) {
		ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder
			.forAddress(properties.getHost(), properties.getPort()).usePlaintext(properties.isPlainText())
			.directExecutor();
		if (properties.getIdleTimeout() > 0) {
			managedChannelBuilder = managedChannelBuilder.idleTimeout(properties.getIdleTimeout(), TimeUnit.SECONDS);
		}
		if (properties.getMaxMessageSize() > 0) {
			managedChannelBuilder = managedChannelBuilder.maxInboundMessageSize(properties.getMaxMessageSize());
		}
		return managedChannelBuilder.build();
	}

	@StreamListener(Processor.INPUT)
	public void process(final Message<?> request) {
		ProtobufMessageBuilder protobufMessageBuilder = new ProtobufMessageBuilder();

		org.springframework.cloud.stream.app.grpc.message.Message protobufMessage = properties.isIncludeHeaders() ?
			protobufMessageBuilder.fromMessage(request).build() :
			protobufMessageBuilder.withPayload(request.getPayload()).build();

		processorStub
			.process(protobufMessage, new StreamObserver<org.springframework.cloud.stream.app.grpc.message.Message>() {

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

	@Bean
	ProcessorGrpc.ProcessorBlockingStub pingStub() {
		return ProcessorGrpc.newBlockingStub(grpcChannel);
	}

	@Bean
	public HealthIndicator sideCarHealthIndicator() {
		return new HealthIndicator() {
			@Override
			public Health health() {
				try {
					Status status = pingStub().ping(Empty.getDefaultInstance());
					return Health.status(status.getMessage()).build();
				}
				catch (Exception e) {
					return Health.down().build();
				}

			}
		};
	}
}
