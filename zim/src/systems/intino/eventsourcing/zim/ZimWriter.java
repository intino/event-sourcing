package systems.intino.eventsourcing.zim;

import systems.intino.eventsourcing.message.Message;
import systems.intino.eventsourcing.message.MessageWriter;
import io.intino.alexandria.resourcecleaner.DisposableResource;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

public class ZimWriter implements AutoCloseable {

	private final MessageWriter writer;
	private final DisposableResource resource;

	public ZimWriter(File file) throws IOException {
		this(Zim.compressing(new BufferedOutputStream(new FileOutputStream(file))));
	}

	public ZimWriter(OutputStream out) throws IOException {
		MessageWriter writer = new MessageWriter(Zim.compressing(out));
		this.writer = writer;
		this.resource = DisposableResource.whenDestroyed(this).thenClose(writer);
	}

	public void write(Message... messages) throws IOException {
		write(Arrays.stream(messages));
	}

	public void write(Collection<Message> messages) throws IOException {
		write(messages.stream());
	}

	public void write(Stream<Message> messages) throws IOException {
		Iterator<Message> iterator = messages.iterator();
		while(iterator.hasNext()) write(iterator.next());
	}

	public void write(Message message) throws IOException {
		writer.write(message);
	}

	public void flush() throws IOException {
		writer.flush();
	}

	@Override
	public void close() {
		resource.close();
	}
}
