# NSDS - Akka exercises

My solutions to the Akka lab exercises.
Each exercise is in its own package under `src/main/java/com/`; copy it into the course `nsds-akka` project and run its `Main`.

## Exercises 3 + 5 (`com.ex3_5`)

Contact list server with a supervisor; see the [statements](src/main/java/com/ex3_5/README.md).

## Exercise 6 (`com.ex6`)

- Write a client actor that asks a server actor to uppercase one TextMsg at a time
  - The server actor replies after a short delay that emulates some longer processing
  - While processing, the server enters a "busy" behavior and buffers new requests arriving meanwhile
  - This means it must NOT block while processing
  - When processing a message completes, the server sends the reply to the client, becomes ready again, and processes buffered messages
  - For testing, create two clients issuing a series of commands in parallel
- You may use `getContext().system().scheduler().scheduleOnce()` in the solution, for example:
  `…scheduleOnce(delay, recipient, msg, getContext().dispatcher(), sender);`
