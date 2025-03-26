package systems.intino.eventsourcing.zim;

import io.intino.alexandria.iteratorstream.ResourceIteratorStream;
import systems.intino.eventsourcing.message.Message;
import systems.intino.eventsourcing.message.MessageReader;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Stream;

@SuppressWarnings({"all"})
public class ZimStream extends ResourceIteratorStream<Message> {

	public static ZimStream sequence(File first, File... rest) throws IOException {
		ZimStream[] streams = new ZimStream[1 + rest.length];
		streams[0] = ZimStream.of(first);
		for (int i = 0; i < rest.length; i++) streams[i + 1] = ZimStream.of(rest[i]);
		return new ZimStream(Arrays.stream(streams).flatMap(Function.identity()).iterator());
	}

	public static ZimStream sequence(Stream<Message>... streams) {
		return new ZimStream(Arrays.stream(streams).flatMap(Function.identity()).iterator());
	}

	public static ZimStream of(File file) throws IOException {
		return new ZimStream(!file.exists() ? Collections.emptyIterator() : readerOf(Zim.decompressing(fileInputStream(file))));
	}

	public static ZimStream of(InputStream is) throws IOException {
		return new ZimStream(readerOf(Zim.decompressing(is)));
	}

	public static ZimStream of(String text) {
		return ZimStream.of(new MessageReader(text));
	}

	public static ZimStream of(Message... messages) {
		return new ZimStream(Arrays.stream(messages).iterator());
	}

	public static ZimStream of(Collection<Message> messages) {
		return new ZimStream(messages.iterator());
	}

	public static ZimStream of(Stream<Message> messages) {
		return new ZimStream(messages.iterator());
	}

	public static ZimStream of(MessageReader reader) {
		return new ZimStream(reader.iterator());
	}

	public ZimStream(Iterator<Message> iterator) {
		super(iterator);
	}

	private static MessageReader readerOf(InputStream is) {
		return new MessageReader(is);
	}

	private static BufferedInputStream fileInputStream(File file) throws IOException {
		return new BufferedInputStream(new FileInputStream(file));
	}
}