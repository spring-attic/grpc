package org.springframework.cloud.stream.app.grpc.test.support;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author David Turanski
 **/
public abstract class AbstractProcessorTest {

	private ManagedChannel inProcessChannel;

	private Executor executor = Executors.newCachedThreadPool();

	protected abstract AbstractGrpcServer getServer();


	@Before
	public void setUp() throws Exception {
		getServer().start();
		inProcessChannel = InProcessChannelBuilder.forName(getServer().getName()).executor(executor).build();
	}

	@After
	public void tearDown() throws Exception {

		inProcessChannel.shutdown();
		inProcessChannel.awaitTermination(1, TimeUnit.SECONDS);
		getServer().stop();
	}

	protected ManagedChannel getChannel() {
		return inProcessChannel;
	}

}
