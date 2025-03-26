package systems.intino.eventsourcing.message;

import java.io.*;
import java.nio.charset.Charset;

public class MessageWriter implements AutoCloseable {

	private final BufferedWriter writer;

	public MessageWriter(OutputStream os) {
		this(os, Charset.defaultCharset());
	}

	public MessageWriter(OutputStream os, Charset charset) {
		this.writer = new BufferedWriter(new OutputStreamWriter(os, charset));
	}

	public void write(Message message) throws IOException {
		writer.write(message.toString());
		writer.newLine();
	}

	public void flush() throws IOException {
		writer.flush();
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}
}