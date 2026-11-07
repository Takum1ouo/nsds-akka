# NSDS Akka - Lab 2 Exercise 6

Busy server that turns text into upper case.

- The server handles one message at a time. Processing takes 2 seconds (`scheduleOnce` + `DoneMsg`, no `Thread.sleep`).
- While busy, new messages are stashed and released with `unstashAll()` when it is done.
- The server switches between two states with `become`: `ready()` and `busy()`.
- Two clients send messages; each one only gets its own replies back.

## Run

Copy `src/main/java/com/ex6` into the course `nsds-akka` project and run `com.ex6.Main`. Press Enter to exit.

## Author

Chuanhao Cai
