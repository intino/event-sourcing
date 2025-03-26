package systems.intino.eventsourcing.event;

import systems.intino.eventsourcing.event.message.MessageEventWriter;
import systems.intino.eventsourcing.event.resource.ResourceEventWriter;
import io.intino.alexandria.logger.Logger;

import java.io.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

public interface EventWriter<T extends Event> extends AutoCloseable {

	static <T extends Event> EventWriter<T> of(File file) throws IOException {
		return EventWriter.of(file, false);
	}

	static <T extends Event> EventWriter<T> of(Event.Format format, File file) throws IOException {
		return EventWriter.of(format, file, false);
	}

	static <T extends Event> EventWriter<T> of(File file, boolean append) throws IOException {
		return EventWriter.of(Event.Format.of(file), file, append);
	}

	@SuppressWarnings("unchecked")
	static <T extends Event> EventWriter<T> of(Event.Format format, File file, boolean append) throws IOException {
		switch(format) {
			case Message: return (EventWriter<T>) new MessageEventWriter(file, append);
			case Resource: return (EventWriter<T>) new ResourceEventWriter(file);
			default: Logger.error("Unknown event format " + Event.Format.of(file));
		}
		return new Empty<>();
	}

	@SuppressWarnings("unchecked")
	static <T extends Event> EventWriter<T> of(Event.Format format, OutputStream outputStream) throws IOException {
		switch(format) {
			case Message: return (EventWriter<T>) new MessageEventWriter(outputStream);
			case Resource: Logger.error("Resource event format not supported for outputStream");
			default: Logger.error("Unknown event format " + format);
		}
		return new Empty<>();
	}

	static <T extends Event> void append(File file, Stream<T> events) throws IOException {
		write(file, true, events);
	}

	static <T extends Event> void append(File file, Collection<T> events) throws IOException {
		write(file, true, events.stream());
	}

	static <T extends Event> void write(File file, Stream<T> events) throws IOException {
		write(file, false, events);
	}

	static <T extends Event> void write(File file, boolean append, Collection<T> events) throws IOException {
		write(file, append, events.stream());
	}

	static <T extends Event> void write(File file, boolean append, Stream<T> events) throws IOException {
		try(EventWriter<T> writer = EventWriter.of(file, append)) {
			writer.write(events);
		}
	}

	static <T extends Event> void write(Event.Format format, OutputStream destination, Collection<T> events) throws IOException {
		write(format, destination, events.stream());
	}

	static <T extends Event> void write(Event.Format format, OutputStream destination, Stream<T> events) throws IOException {
		try(EventWriter<T> writer = EventWriter.of(format, destination)) {
			writer.write(events);
		}
	}

	void write(T event) throws IOException;

	default void write(Stream<T> stream) throws IOException {
		try(stream) {
			Iterator<T> iterator = stream.iterator();
			while (iterator.hasNext()) write(iterator.next());
		}
	}

	default void write(Collection<T> messages) throws IOException {
		write(messages.stream());
	}

	void flush() throws IOException;

	@Override
	void close() throws IOException;

	class IO {
		public static OutputStream open(File file, boolean append) throws IOException {
			return new FileBufferedOutputStream(new FileOutputStream(file, append));
		}

		public static class FileBufferedOutputStream extends BufferedOutputStream {
			public FileBufferedOutputStream(FileOutputStream out) {
				super(out);
			}
		}
	}

	class Empty<T extends Event> implements EventWriter<T> {
		@Override
		public void write(T event) {}
		@Override
		public void write(Stream<T> stream) {}

		@Override
		public void flush() {}

		@Override
		public void close() {}
	}
}
