import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.concurrent.TimeUnit;

/**
 * A simple subscriber that connects to a channel, subscribes to a stream, and prints all messages received from the stream.
 */
public class Subscriber {
    public static void main(String[] args) throws InterruptedException {
        // get configs
        //  -DaeronPlayground.dir=/tmp/media-driver-1
        String aeronDir = System.getProperty("aeronPlayground.dir");
        //  -DaeronPlayground.channel="aeron:ipc"
        String aeronChannel = System.getProperty("aeronPlayground.channel");
        //  -DaeronPlayground.stream="51"
        int aeronStream = Integer.parseInt(System.getProperty("aeronPlayground.stream"));

        // create the configuration
        final Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(aeronDir);
        int fragmentLimit = 10; // TODO: not sure what fragment limit is
        IdleStrategy idleStrategy = new BackoffIdleStrategy(100, 10, TimeUnit.SECONDS.toNanos(1), TimeUnit.SECONDS.toNanos(10));

        // connect to the media driver using the configuration
        try (final Aeron aeron = Aeron.connect(ctx); final Subscription subscription = aeron.addSubscription(aeronChannel, aeronStream)) {

            /*
            while (!subscription.isConnected()) {
                System.out.println("Waiting for publisher...");
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            }
            */

            FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {
                // copy the bytes from over to a new buffer -> TODO: do we need to do this?
                byte[] messageBytes = new byte[length];
                buffer.getBytes(offset, messageBytes);
                String message = new String(messageBytes);

                System.out.format("Received message: %s\n", message);
            };

            while (true) {
                final int numFragmentsRead = subscription.poll(fragmentHandler, fragmentLimit);

                // idle before polling again
                idleStrategy.idle(numFragmentsRead);
            }
        }
    }
}
