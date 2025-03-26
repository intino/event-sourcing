# EventStream

The `EventStream` interface is a base interface for managing events in a program.

## Methods

### current()
Returns the current `Event` in the stream.

### next()
Advances the stream to the next `Event` and returns it.

### hasNext()
Returns `true` if there are more events in the stream, `false` otherwise.

### forEachRemaining(Consumer<Event> action)
Applies the `Consumer` action to each remaining event in the stream.

## Inner Classes

### Merge
Implements `EventStream` and provides a mechanism for merging multiple `EventStream`s into a single stream.

#### Merge(EventStream... inputs)
Constructs a `Merge` instance from the given `EventStream` inputs.

#### of(EventStream... inputs)
Creates a `Merge` instance from the given `EventStream` inputs.

### Sequence
Implements `EventStream` and provides a mechanism for organizing multiple `EventStream`s into a sequential stream.

#### Sequence(EventStream... inputs)
Constructs a `Sequence` instance from the given `EventStream` inputs.

#### of(EventStream... inputs)
Creates a `Sequence` instance from the given `EventStream` inputs.

### Empty
Implements `EventStream` and provides an empty `EventStream`.

# EventWriter Class

The `EventWriter` class is a utility class for writing events to a file. It provides a convenient interface for writing events from various sources, such as arrays, lists, or streams.

## Usage

To use the `EventWriter` class, you need to first create an instance of it, passing in the target file as a parameter. The following code demonstrates this:

```java
File file = new File("events.txt");
EventWriter writer = new EventWriter(file);
```

Once you have an instance of the `EventWriter` class, you can write events to the target file using any of the put methods. The following code demonstrates this:

```java
Event event1 = new Event("type");
Event event2 = new Event("type");
Event event3 = new Event("type");
writer.put(event1, event2, event3);
```
or
```java
List<Event> events = Arrays.asList(event1, event2, event3);
writer.put(events);
```
or
```java
EventStream eventStream = new EventReader(Stream.of(event1, event2, event3));
writer.put(eventStream);
```


