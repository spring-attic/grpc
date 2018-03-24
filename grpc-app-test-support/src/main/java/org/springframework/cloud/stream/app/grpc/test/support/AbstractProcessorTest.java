package org.springframework.cloud.stream.app.grpc.test.support;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

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
