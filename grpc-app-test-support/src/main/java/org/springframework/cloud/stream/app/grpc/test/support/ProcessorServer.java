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

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.springframework.cloud.stream.app.grpc.processor.ProcessorGrpc;
import org.springframework.cloud.stream.app.grpc.processor.ProcessorProtos.Message;
import org.springframework.cloud.stream.app.grpc.processor.ProcessorProtos.Status;
import org.springframework.cloud.stream.app.grpc.processor.ReactorProcessorGrpc;
import org.springframework.cloud.stream.app.grpc.support.ProtobufMessageBuilder;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author David Turanski
 **/
public class ProcessorServer {

	private final Server server;

	public ProcessorServer(ServerBuilder<?> serverBuilder) {

		server = serverBuilder
			.addService(new ProcessorService())
			.addService(new ReactiveProcessorService())
			.build();

	}

	/**
	 * Start serving requests.
	 */
	public void start() throws IOException {
		server.start();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				// Use stderr here since the logger may has been reset by its JVM shutdown hook.
				System.err.println("*** shutting down gRPC server since JVM is shutting down");
				ProcessorServer.this.stop();
				System.err.println("*** server shut down");
			}
		});
	}

	/**
	 * Stop serving requests and shutdown resources.
	 */
	public void stop() {
		if (server != null) {
			server.shutdown();
		}
	}

	public static class ProcessorService extends ProcessorGrpc.ProcessorImplBase {
		private static Integer MAX_PINGS = 3;
		private AtomicInteger pingCount = new AtomicInteger(0);

		@Override
		public void process(Message message, StreamObserver<Message> observer) {
			String result = new String(message.getPayload().toStringUtf8());

			Message response = new ProtobufMessageBuilder().withPayload(result.toUpperCase().getBytes())
				.withProtobufHeaders(message.getHeadersMap()).build();

			observer.onNext(response);
			observer.onCompleted();
		}

		public void ping(com.google.protobuf.Empty request, io.grpc.stub.StreamObserver<Status> responseObserver) {
			System.out.println(pingCount.get());
			if (pingCount.incrementAndGet() == MAX_PINGS) {
				pingCount.set(0);
				throw new StatusRuntimeException(io.grpc.Status.UNAVAILABLE);
			}
			responseObserver.onNext(Status.newBuilder().setMessage("alive").build());
			responseObserver.onCompleted();
		}

		@Override
		public StreamObserver<Message> stream(final StreamObserver<Message> responseObserver) {
			System.out.println("Calling stream with StreamObserver");
			return new StreamObserver<Message>() {
				@Override
				public void onNext(Message message) {
					responseObserver.onNext(Message.newBuilder()
						.setPayload(ByteString.copyFromUtf8(message.getPayload().toStringUtf8().toUpperCase()))
						.build());
				}

				@Override
				public void onError(Throwable throwable) {
					responseObserver.onError(throwable);
				}

				@Override
				public void onCompleted() {
					responseObserver.onCompleted();
				}
			};
		}
	}

	public static class ReactiveProcessorService extends ReactorProcessorGrpc.ProcessorImplBase {
		@Override
		public Flux<Message> stream(Flux<Message> input) {
			System.out.println("Calling stream with Flux");
			return input.log().map(m -> Message.newBuilder()
				.setPayload(ByteString.copyFromUtf8(m.getPayload().toStringUtf8().toUpperCase())).build());
		}
	}
}
