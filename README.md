# Why didnt the timer fire?

The stateful pardo is supposed to act like a SessionWindow, accumulating elements 
until no events are received for [Duration], then output and clear its state. But
this does not appear to be happening? Timer is configured in event time, all
events are timestamped in event time. Debug statements clearly show watermark is
passed the timer expiry, but it does not fire until the program ends. 

Given the test input here, I would expect it to fire once after the second event,
produce output, clear state, then fire again after the last event and do the same.
Yet all output gets accumulated and fired together.

To run the test: `./gradlew -i --rerun-tasks test`