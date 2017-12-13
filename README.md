# Multi Producer Sink

Allows multiple different SubSinks to each write to the same Sink.

This deals with interior mutability, since usually only one mutable reference to a Sink is allowed. This also needs to keep a queue of Tasks so that each SubSink as the correct nonblocking behaviour.

On an error, the SubSinks only return a reference to the Error.