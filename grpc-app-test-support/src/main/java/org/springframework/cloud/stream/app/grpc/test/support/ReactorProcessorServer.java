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

import com.google.protobuf.ByteString;
import io.grpc.BindableService;
import org.springframework.cloud.stream.app.grpc.processor.ProcessorProtos.Message;
import org.springframework.cloud.stream.app.grpc.processor.ReactorProcessorGrpc;
import reactor.core.publisher.Flux;

/**
 * @author David Turanski
 **/
public class ReactorProcessorServer extends AbstractGrpcServer {

	@Override
	protected BindableService getService() {
		return new ReactorProcessorService();
	}

	public static class ReactorProcessorService extends ReactorProcessorGrpc.ProcessorImplBase {
		@Override
		public Flux<Message> stream(Flux<Message> input) {
			return input.map(m -> Message.newBuilder()
				.setPayload(ByteString.copyFromUtf8(m.getPayload().toStringUtf8().toUpperCase()))
				.build());
		}
	}
}
