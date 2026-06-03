package publisher;

import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.shell.core.ShellRunner;
import org.springframework.shell.core.command.annotation.*;

import java.util.List;

/**
 * A simple publisher that connects to a `channel`, and pushes a message to a `stream` once every second.
 */
@EnableCommand({Publisher.IntervalCommands.class, Publisher.ArchiveCommands.class, Publisher.AeronPublicationCommands.class})
public class Publisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(Publisher.class);

    private static final PublisherAgent agent = new PublisherAgent();

    public static void main(String[] args) throws Exception {
        // start the publisher agent
        IdleStrategy idleStrategy = new BackoffIdleStrategy();
        AgentRunner agentRunner = new AgentRunner(idleStrategy, Throwable::printStackTrace, null, agent);
        AgentRunner.startOnThread(agentRunner);

        // start up an interactive console for us to interact with the publisher app as its running
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Publisher.class);
        ShellRunner runner = context.getBean(ShellRunner.class);
        try {
            runner.run(args);
        } finally {
            CloseHelper.closeAll(agentRunner, context);
            LogManager.shutdown();
        }
    }

    @CommandGroup(name = "Interval Commands", prefix = "i")
    public static final class IntervalCommands {
        @Command(name = "set", description = "Set the interval between messages sent by the publisher, in milliseconds.")
        public String interval(
                @Argument(index = 0, description = "interval, in milliseconds") int interval
        ) {
            agent.setIntervalInMs(interval);
            String output = String.format("Setting message interval to: %s ms", interval);
            LOGGER.info(output);
            return output;
        }

        @Command(name = "show", description = "Set the interval between messages sent by the publisher, in milliseconds.")
        public String show(
        ) {
            return String.format("Current message interval: %s ms", agent.getIntervalInMs());
        }
    }

    @CommandGroup(name = "Aeron Client Publication Commands", prefix = "p")
    public static final class AeronPublicationCommands {
        @Command(name = "list", description = "List all Aeron publications.")
        public String listAeronPublications() {
            return String.format("Publication: %s", agent.getPublication());
        }
    }

    @CommandGroup(name = "Archive Client Commands", prefix = "a")
    public static final class ArchiveCommands {
        @Command(name = "list", description = "List all archive recordings for the current published stream.")
        public String listArchiveRecordings() {
            List<RecordingDetails> recordingDetailsList = agent.getListOfRecordings();
            String output = String.format("Recording details: %s", recordingDetailsList);
            LOGGER.info(output);
            return output;
        }

        @Command(name = "stop", description = "Stop recording.")
        public String stopArchiveRecording() {
            boolean stoppedRecording = agent.stopRecording();
            if (stoppedRecording) {
                return "Stopped recording";
            } else {
                return "Failed to stop recording";
            }
        }
    }
}
