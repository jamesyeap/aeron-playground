package publisher;

import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple publisher that connects to a `channel`, and pushes a message to a `stream` once every second.
 */
public class Publisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(Publisher.class);

    public static void main(String[] args) {
        IdleStrategy idleStrategy = new BackoffIdleStrategy();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        AgentRunner agentRunner = new AgentRunner(idleStrategy, Throwable::printStackTrace, null, new PublisherAgent());

        AgentRunner.startOnThread(agentRunner);
        barrier.await();

        CloseHelper.closeAll(agentRunner, barrier);
        LogManager.shutdown();
    }

}
