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
import org.springframework.shell.core.command.annotation.Command;
import org.springframework.shell.core.command.annotation.EnableCommand;
import org.springframework.shell.core.command.annotation.Option;

import java.util.Set;

/**
 * A simple publisher that connects to a `channel`, and pushes a message to a `stream` once every second.
 */
@EnableCommand(Publisher.class)
public class Publisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(Publisher.class);

    public static void main(String[] args) throws Exception {
        // start the publisher agent
        IdleStrategy idleStrategy = new BackoffIdleStrategy();
        AgentRunner agentRunner = new AgentRunner(idleStrategy, Throwable::printStackTrace, null, new PublisherAgent());

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

    @Command(name = "hello", description = "Say hello to a given name", group = "Greetings",
            help = "A command that greets the user with 'Hello ${name}!'. Usage: hello [-n | --name]=<name>")
    public void sayHello(@Option(shortName = 'n', longName = "name", description = "the name of the person to greet",
            defaultValue = "World") String name) {
        System.out.println("Hello " + name + "!");
    }
}
