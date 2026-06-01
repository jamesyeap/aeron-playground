package subscriber;

import org.agrona.CloseHelper;
import org.agrona.concurrent.*;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A simple subscriber that connects to a channel, subscribes to a stream, and prints all messages received from the stream.
 */
public class Subscriber {
    private static final Logger LOGGER = LoggerFactory.getLogger(Subscriber.class);

    public static void main(String[] args) {
        IdleStrategy idleStrategy = new BackoffIdleStrategy(100, 10, TimeUnit.SECONDS.toNanos(1), TimeUnit.SECONDS.toNanos(10));
        ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        AgentRunner runner = new AgentRunner(idleStrategy, Throwable::printStackTrace, null, new SubscriberAgent(idleStrategy));

        AgentRunner.startOnThread(runner);
        barrier.await();

        CloseHelper.closeAll(runner, barrier);
        LogManager.shutdown();
    }
}
