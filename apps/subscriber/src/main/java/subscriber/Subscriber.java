package subscriber;

import org.agrona.CloseHelper;
import org.agrona.concurrent.*;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.shell.core.ShellRunner;
import org.springframework.shell.core.command.annotation.Command;
import org.springframework.shell.core.command.annotation.CommandGroup;
import org.springframework.shell.core.command.annotation.EnableCommand;

import java.util.concurrent.TimeUnit;

/**
 * A simple subscriber that connects to a channel, subscribes to a stream, and prints all messages received from the stream.
 */
@EnableCommand({Subscriber.ProcessorCommands.class, Subscriber.SubscriptionCommands.class})
public class Subscriber {
    private static final Logger LOGGER = LoggerFactory.getLogger(Subscriber.class);

    private static SubscriberAgent agent;

    public static void main(String[] args) throws Exception {
        // start the subscriber agent
        IdleStrategy idleStrategy = new BackoffIdleStrategy(100, 10, TimeUnit.SECONDS.toNanos(1), TimeUnit.SECONDS.toNanos(10));
        agent = new SubscriberAgent(idleStrategy);

        AgentRunner agentRunner = new AgentRunner(idleStrategy, Throwable::printStackTrace, null, agent);
        AgentRunner.startOnThread(agentRunner);

        // start up an interactive console for us to interact with the subscriber app as its running
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Subscriber.class);
        ShellRunner runner = context.getBean(ShellRunner.class);
        try {
            runner.run(args);
        } finally {
            CloseHelper.closeAll(agentRunner, context);
            LogManager.shutdown();
        }
    }

    @CommandGroup(name = "Subscription Commands", prefix = "s")
    public static final class SubscriptionCommands {
        @Command(name = "show", description = "Show details about the subscription.")
        public String showSubscription() {
            return String.format("Subscription: %s", agent.getSubscription());
        }
    }

    @CommandGroup(name = "Processor Commands", prefix = "p")
    public static final class ProcessorCommands {
        @Command(name = "stop", description = "Start processing messages from the subscription.")
        public String stopProcessing() {
            agent.setShouldProcess(false);

            String output = "Stopped processing messages";
            LOGGER.info(output);
            return output;
        }

        @Command(name = "start", description = "Start processing messages from the subscription.")
        public String startProcessing() {
            agent.setShouldProcess(true);

            String output = "Started processing messages";
            LOGGER.info(output);
            return output;
        }

        @Command(name = "show", description = "Show status of processing messages from the subscription.")
        public String showProcessingStatus(
        ) {
            boolean shouldProcess = agent.isShouldProcess();
            return String.format("Processing messages: %b", shouldProcess);
        }
    }
}
