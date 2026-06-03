# Aeron Playground

A simple toy application for me to explore how Aeron Transport, Aeron Archive and Aeron Cluster works.

# Overview

It has 4 main classes:

1. StartMediaDriver
2. StartArchiver
3. Publisher
4. subscriber.Subscriber

## StartMediaDriver

Creates an Aeron Media Driver, with the `aeronDir` set to `/tmp/media-driver-1`.

The term buffer length is configurable in `mediaDriver.sh`.

## StartArchiver

Creates an Aeron Archive, with the `archiveDir` set to `/tmp/archive-media-driver-1`

## Publisher

Initializes a counter to `0` - at each time interval (configurable via CLI, default is `1 second`), it:

- Sends the value of the counter to (channel:`localhost:12345`, stream:`51`).
- Then it increments the counter by 1.

### Redundancy

If the publisher is restarted, it fetches all the values that it has previously sent from the Aeron Archive, and
initializes the counter to the latest value that it previously sent (instead of `0`).

On startup, the publisher requests the Aeron Archive to start or extend a recording on (channel:`localhost:12345`,
stream:`51`).

- This recording will be replayed by both the Publisher and Subscriber on startup to get the latest state.

### Commands

| Command  | Description                                        |
|----------|----------------------------------------------------|
| `p list` | Show all publications                              |
| `i set`  | Set the message publishing rate, in milliseconds.  |
| `i show` | Show the message publishing rate, in milliseconds. |

## Subscriber

On startup, the subscriber checks if Aeron Archive has any recordings from (channel:`localhost:12345`, stream:`51`).

- If it does, it requests a replay from the Aeron Archive, and then creates a `replay merge` subscription.
- Otherwise, it just creates a plain subscription to the stream.

The subscriber just prints the value in each message to lgos.

### Commands

| Command   | Description                                       |
|-----------|---------------------------------------------------|
| `s show`  | Show the details of the subscription              |
| `p start` | Resume processing messages from the subscription. |
| `p stop`  | Stop processing messages from the subscription.   |
| `p show`  | Show status.                                      |

# Starting the components

Run `./gradlew build` to build the project.

Then, run all the scripts in `scripts` in this order:

- `scripts/mediaDriver.sh`
- `scripts/archiver.sh`
- `scripts/publisher.sh`
- `scripts/subscriber.sh`

# Interesting things to explore

## Backpressure

To explore how Aeron handles backpressure, you can

- Stop the Subscriber from processing messages with `p stop`.
- Increase the message publishing rate in the Publisher with `i set 50`

Check the Aeron counter stats using the tools scripts:

```bash
./aeronStat.sh /tmp/media-driver-1
./streamStat.sh /tmp/media-driver-1 | grep streamId=51
./backlogStat.sh /tmp/media-driver-1
```

- note that there will be two subscribers to stream 51: the Subscriber itself, and Aeron Archive

# Nuances

Note that in this setup, there is only a single Publisher publishing to (channel:`localhost:12345`, stream:`51`), so
extending the archive Recording is straightforward.

However, if there are multiple Publishers to the same channel and stream, things can get a bit more complicated:

- at each time, there can only be a single Publisher writing to a Recording (a Recording cannot track two Sessions)

So when a Publisher starts up, it needs a way to identify which recording belonged to it.

- one approach would be to write the `SessionId` to disk - on startup, the Publisher queries for Recording(s) (ideally
  there should only be one, as Recordings should be extended where possible) that were tracking its previous session.
- as `SessionId` is guaranteed to be unique on each node, no two Publishers will request to extend the same Recording.

# Interesting resources
- [The Aeron File](https://theaeronfiles.com/overview/)
  - unofficial documentation on Aeron written by Chris Smith
    - which deep-dives into the internals of Aeron Transport, Aeron Archive and Aeron Cluster