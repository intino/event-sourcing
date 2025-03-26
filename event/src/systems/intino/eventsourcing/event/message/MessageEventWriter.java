package systems.intino.eventsourcing.event.message;

import systems.intino.eventsourcing.zim.Zim;
import systems.intino.eventsourcing.zim.ZimWriter;
import systems.intino.eventsourcing.event.EventWriter;

import java.io.*;

public class MessageEventWriter implements EventWriter<MessageEvent> {

	private final ZimWriter writer;

	public MessageEventWriter(File file) throws IOException {
		this(file, true);
	}

	public MessageEventWriter(File file, boolean append) throws IOException {
		this(IO.open(file, append));
	}

	public MessageEventWriter(OutputStream destination) throws IOException {
		this.writer = new ZimWriter(Zim.compressing(destination));
	}

	@Override
	public void write(MessageEvent event) throws IOException {
		writer.write(event.toMessage());
	}

	@Override
	public void flush() throws IOException {
		writer.flush();
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}
}