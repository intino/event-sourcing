package systems.intino.eventsourcing.event.message;

import systems.intino.eventsourcing.zim.ZimStream;
import systems.intino.eventsourcing.event.EventReader;
import systems.intino.eventsourcing.message.Message;
import systems.intino.eventsourcing.message.MessageReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.stream;

@SuppressWarnings("all")
public class MessageEventReader implements EventReader<MessageEvent> {

	private final Iterator<MessageEvent> iterator;

	public MessageEventReader(File file) throws IOException {
		this(new MessageToEventIterator(ZimStream.of(file).iterator()));
	}

	public MessageEventReader(InputStream is) throws IOException {
		this(new MessageToEventIterator(ZimStream.of(is).iterator()));
	}

	public MessageEventReader(String text) {
		this(new MessageToEventIterator(new MessageReader(text).iterator()));
	}

	public MessageEventReader(MessageEvent... events) {
		this(stream(events));
	}

	public MessageEventReader(List<MessageEvent> events) {
		this(events.stream());
	}

	public MessageEventReader(Stream<MessageEvent> stream) {
		this(stream.sorted().iterator());
	}

	public MessageEventReader(Iterator<MessageEvent> iterator) {
		this.iterator = iterator;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public MessageEvent next() {
		return iterator.next();
	}

	@Override
	public void close() throws Exception {
		if(iterator instanceof AutoCloseable) ((AutoCloseable) iterator).close();
	}

	private static class MessageToEventIterator implements Iterator<MessageEvent>, AutoCloseable {

		private final Iterator<Message> source;

		public MessageToEventIterator(Iterator<Message> source) {
			this.source = source;
		}

		@Override
		public void close() throws Exception {
			if(source instanceof AutoCloseable) ((AutoCloseable) source).close();
		}

		@Override
		public boolean hasNext() {
			return source.hasNext();
		}

		@Override
		public MessageEvent next() {
			return new MessageEvent(source.next());
		}
	}
}
