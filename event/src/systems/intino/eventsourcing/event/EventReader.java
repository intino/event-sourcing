package systems.intino.eventsourcing.event;

import systems.intino.eventsourcing.event.message.MessageEventReader;
import systems.intino.eventsourcing.event.resource.ResourceEventReader;
import io.intino.alexandria.logger.Logger;

import java.io.*;
import java.util.Iterator;

public interface EventReader<T extends Event> extends Iterator<T>, AutoCloseable {

	@SuppressWarnings("unchecked")
	static <T extends Event> EventReader<T> of(File file) throws IOException {
		return EventReader.of(Event.Format.of(file), file);
	}

	@SuppressWarnings("unchecked")
	static <T extends Event> EventReader<T> of(Event.Format format, File file) throws IOException {
		if(!file.exists()) return new Empty<>();
		switch(format) {
			case Message: return (EventReader<T>) new MessageEventReader(file);
			case Resource: return (EventReader<T>) new ResourceEventReader(file);
			default: Logger.error("Unknown event format " + Event.Format.of(file));
		}
		return new Empty<>();
	}

	@SuppressWarnings("unchecked")
	static <T extends Event> EventReader<T> of(Event.Format format, InputStream inputStream) throws IOException {
		switch(format) {
			case Message: return (EventReader<T>) new MessageEventReader(inputStream);
			case Resource: Logger.error("Resource event format not supported for outputStream");
			default: Logger.error("Unknown event format " + format);
		}
		return new Empty<>();
	}

	class IO {
		public static InputStream open(File file) throws IOException {
			return new BufferedInputStream(new FileInputStream(file));
		}
	}

	class Empty<T extends Event> implements EventReader<T> {
		@Override
		public void close() {}

		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public T next() {
			return null;
		}
	}
}
