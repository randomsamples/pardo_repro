# Why didnt the timer fire?

The stateful pardo is supposed to act like a SessionWindow, accumulating elements 
until no events are received for [Duration], then output and clear its state. But
this does not appear to be happening? Timer is configured in event time, all
events are timestamped in event time. Debug statements clearly show watermark is
passed the timer expiry, but it does not fire until the program ends.

To run the test: `./gradlew -i --rerun-tasks test`