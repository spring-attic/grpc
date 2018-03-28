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

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.AfterClass;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author David Turanski
 **/
public abstract class AbstractProcessorTest {

	private static ManagedChannel inProcessChannel;

	private static Executor executor = Executors.newCachedThreadPool();

	protected static AbstractGrpcServer server;

	public static void init(AbstractGrpcServer grpcServer) throws Exception {
		server = grpcServer;
		server.start();
		inProcessChannel = InProcessChannelBuilder.forName(server.getName()).executor(executor).build();
	}

	@AfterClass
	public static void tearDown() throws Exception {

		inProcessChannel.shutdown();
		inProcessChannel.awaitTermination(1, TimeUnit.SECONDS);
		server.stop();
	}

	protected static ManagedChannel getChannel() {
		return inProcessChannel;
	}

}
