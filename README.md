# Aeron Playground

A simple toy application for me to explore how Aeron Transport, Aeron Archive and Aeron Cluster works.

It has 4 main classes:

1. StartMediaDriver
2. StartArchiver
3. Publisher
4. Subscriber

# StartMediaDriver

Creates an Aeron Media Driver, with the `aeronDir` set to `/tmp/media-driver-1`.

# StartArchiver

Creates an Aeron Archive, with the `archiveDir` set to `/tmp/archive-media-driver-1`

# Publisher

Initializes a counter to 0 - every second, it:

- Sends the value of the counter to a channel `localhost:12345`, stream `51`.
- Increments the counter by 1.

Note: if the publisher is restarted, it fetches all the values that it has previously sent from the Aeron Archive, and
initializes the counter to the latest value that it previously sent (instead of 0).

# Subscriber

Receives the value of the counter from the channel at `localhost:12345`, stream `51`, and prints it to logs.
