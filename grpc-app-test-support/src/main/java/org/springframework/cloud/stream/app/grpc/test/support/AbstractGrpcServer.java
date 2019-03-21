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

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * @author David Turanski
 **/
public abstract class AbstractGrpcServer {

	private final Server server;
	private final Log logger = LogFactory.getLog(this.getClass());
	private final String name;

	protected AbstractGrpcServer() {
		name = getClass().getName();
		ServerBuilder<?> serverBuilder = InProcessServerBuilder.forName(name).directExecutor();
		this.server = serverBuilder.addService(getService()).build();
	}

	protected abstract BindableService getService();

	public String getName() {
		return name;
	}

	/**
	 * Start serving requests.
	 */
	public void start() throws IOException {
		server.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info(String.format("*** shutting down gRPC server [%s] since JVM is shutting down", name));
			AbstractGrpcServer.this.stop();
			logger.info(String.format("*** server [%s] shut down",name));
		}));
	}

	/**
	 * Stop serving requests and shutdown resources.
	 */
	public void stop() {
		if (server != null) {
			server.shutdown();
		}
	}
}
