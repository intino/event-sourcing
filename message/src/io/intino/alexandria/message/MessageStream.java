package systems.intino.eventsourcing.message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MessageStream implements Iterator<String>, AutoCloseable {

	private final BufferedReader reader;
	private final StringBuilder buffer;

	public MessageStream(InputStream stream) {
		this(stream, Charset.defaultCharset());
	}

	public MessageStream(InputStream stream, Charset charset) {
		this.reader = new BufferedReader(new InputStreamReader(stream, charset));
		this.buffer = new StringBuilder(64);
		init();
	}

	@Override
	public boolean hasNext() {
		return buffer.length() != 0;
	}

	@Override
	public String next() {
		try {
			if(buffer.length() == 0) return null;
			String next;
			while((next = reader.readLine()) != null && isNotANewMessage(next)) {
				buffer.append(next).append('\n');
			}
			final String message = buffer.toString();
			saveNextLineForLaterOrCloseReader(next);
			return message;
		} catch (Exception e) {
			throw new MessageException(e.getMessage(), e);
		}
	}

	private void saveNextLineForLaterOrCloseReader(String next) throws IOException {
		buffer.setLength(0);
		if(next != null) {
			buffer.append(next).append('\n');
		} else {
			reader.close();
		}
	}

	public List<String> nextLines() {
		try {
			if(buffer.length() == 0) return new ArrayList<>(0);
			List<String> lines = new ArrayList<>();
			setFirstLine(lines);
			String next;
			while((next = reader.readLine()) != null && isNotANewMessage(next)) {
				lines.add(next);
			}
			saveNextLineForLaterOrCloseReader(next);
			return lines;
		} catch (Exception e) {
			throw new MessageException(e.getMessage(), e);
		}
	}

	private void setFirstLine(List<String> lines) {
		buffer.setLength(buffer.length() - 1);
		lines.add(buffer.toString());
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

	private static boolean isNotANewMessage(String next) {
		return next.isEmpty() || next.charAt(0) != '[';
	}

	private void init() {
		try {
			String line = getFirstLineNonBlank();
			if(line != null) buffer.append(line).append('\n');
			else reader.close();
		} catch (IOException ignored) {}
	}

	private String getFirstLineNonBlank() throws IOException {
		String line = reader.readLine();
		while(line != null && line.isBlank()) line = reader.readLine();
		return line;
	}
}
