# NSDS - Kafka lab 1

My solutions to the first Kafka lab (Exercises 1, 3 and 4).
The code goes into the course `NSDS_kafka_tutorial` project, next to the `basic`, `admin` and `transactional` packages, which it uses for testing.
Tested with Kafka 4.x (KRaft, single broker on `localhost:9092`).

| Package | Exercise | Classes |
|---|---|---|
| `lab1` | 1 | `BasicConsumer` (C1, prints records), `C2` (removes upper case letters, forwards to a new topic) |
| `lab3` | 3 | `C3`: C2 with exactly-once semantics (transactions) |
| `lab4` | 4 | `C4`: per-key message counter with exactly-once semantics and state recovery; `Producer4`: producer with only 5 keys |

## Exercise 1

- Implement a system with two (basic) consumers for the same topic
  - Consumer C1 prints records on the standard output
  - Consumer C2 processes each value (e.g., removes upper case letters) and stores the result to a new topic
    - Without delivery guarantees
- Consider a single instance for each consumer
  - What happens if one consumer fails?
  - What happens if you restart it?

Topics `lab1In` / `lab1Out`, groups `groupC1` / `groupC2`.
Run C1, C2, `basic.BasicConsumer check1 lab1Out`, then `basic.BasicProducer lab1In`.

## Exercise 2

- Now assume that you realize that consumer C2 is too slow
  - It cannot sustain the rate of messages added to the input topic
- How can you improve the performance of the system?
- Experiment with the system
  - What happens if one consumer fails?
  - What happens if you start multiple consumers?

Same code as Exercise 1: create the input topic with several partitions and start several C2 instances in the same group.

## Exercise 3

- Now you want C2 to guarantee exactly-once semantics
  - Each input message should be delivered to the output topic once and only once
- Experiment with the system
  - What happens if one consumer fails?
  - What happens if you start multiple consumers?

`C3` reads with `read_committed`, disables auto commit, and in one transaction sends the output records and commits the consumer offsets (`sendOffsetsToTransaction`).
Topics `lab3In` / `lab3Out`, group `groupC3`.
Check the output with `transactional.TransactionalConsumer check3 lab3Out`.

## Exercise 4

- Modify C2 to store and forward the overall number of messages received for each key
- Consider a single instance of C2
  - What happens in the case of failure?
  - Does your implementation guarantee exactly-once semantics?
- How do your answers change in the case of multiple instances?

`C4` keeps the counts in a map and sends `(key, count)` to the output topic inside the same transaction as the offsets.
On startup it rebuilds the map by reading the output topic from the beginning (`read_committed`), so after a crash the counts continue instead of restarting from 1.
Each instance needs its own transactional id (first argument, default `c4-tx-1`).

Create `lab4In` with 3 partitions first, then run `C4`, `transactional.TransactionalConsumer check4 lab4Out` and `Producer4`.
