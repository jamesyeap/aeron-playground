import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple class that records all messages from a given channel and stream.
 */
public class StartArchiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartArchiver.class);

    public static void main(String[] args) throws InterruptedException {
        String aeronDir = System.getProperty("aeronPlayground.dir");
        String archiveDir = System.getProperty("aeronPlayground.archiveDir");
        String controlRequestChannel = System.getProperty("aeronPlayground.controlRequestChannel");
        int controlRequestStream = Integer.parseInt(System.getProperty("aeronPlayground.controlRequestStream"));
        String replicationChannel = System.getProperty("aeronPlayground.replicationChannel");

        Archive.Context ctx = new Archive.Context()
                .aeronDirectoryName(aeronDir)
                .archiveDirectoryName(archiveDir)
                .controlChannel(controlRequestChannel)
                .controlStreamId(controlRequestStream)
                .replicationChannel(replicationChannel)
                .threadingMode(ArchiveThreadingMode.DEDICATED);

        try (final Archive archive = Archive.launch(ctx)) {
            System.out.format("Started archiver. Archive directory: %s\n", archiveDir);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.format("Shut down archiver. Archive directory: %s\n", archiveDir);
            }));

            // busy spin to stop exiting
            while (true) {
                Thread.sleep(100);
            }
        }
    }
}
