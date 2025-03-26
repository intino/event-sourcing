# EventSession Class

The `EventSession` class provides a way to write event data to a specific session. It uses a `MessageWriter` object to write the data and a `SessionHandler.Provider` to provide the correct `MessageWriter` object. The class also provides a `flush` method to force the data to be written to the session and a `close` method to close the session.

## Class Members
- `Map<Fingerprint, MessageWriter> writers`: a map to store the `MessageWriter` objects.
- `SessionHandler.Provider provider`: a provider to provide the correct `MessageWriter` object.
- `int autoFlush`: the maximum number of events to be written before a flush is triggered.
- `AtomicInteger count`: a counter for the number of events written.

## Class Constructors
- `EventSession(SessionHandler.Provider provider)`: creates an `EventSession` object with a provider and the default value for `autoFlush` (1,000,000).
- `EventSession(SessionHandler.Provider provider, int autoFlush)`: creates an `EventSession` object with a provider and a specific value for `autoFlush`.

## Class Methods
- `void put(String tank, Timetag timetag, Event... events)`: writes an array of `Event` objects to a specific session identified by a `tank` and a `timetag`. The method triggers a flush if the number of events written reaches the value of `autoFlush`.
- `void put(String tank, Timetag timetag, Stream<Event> eventStream)`: writes a stream of `Event` objects to a specific session identified by a `tank` and a `timetag`.
- `void flush()`: forces the data to be written to the session.
- `void close()`: closes the session.