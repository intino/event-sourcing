package io.intino.performance.impl;

import systems.intino.eventsourcing.message.MessageException;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;

public class MessageStreamOld implements Iterator<String>, AutoCloseable {
	private final Reader reader;
	private char[] buffer;
	private int index;
	private int last;

	public MessageStreamOld(InputStream stream) {
		this(stream, Charset.defaultCharset());
	}

	public MessageStreamOld(InputStream stream, Charset charset) {
		this.reader = new BufferedReader(new InputStreamReader(stream, charset));
		this.buffer = new char[1024];
		init();
	}

	@Override
	public boolean hasNext() {
		return last != -1;
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public String next() {
		if (last == -1) return null;
		buffer[0] = '[';
		index = 1;
		try {
			char current = 0;
			int r;
			while ((r = reader.read()) != -1 && (last != '\n' || r != '[')) {
				if((char)r == '\n' && current == '\r') {
					last = buffer[index - 1] = current = '\n';
					continue;
				}
				current = (char) r;
				append(current);
				last = current;
			}
			last = r;
		} catch (IOException e) {
			throw new MessageException(e.getMessage(), e);
		}
		return index == 1 ? "" : new String(buffer, 0, index);
	}

	private void init() {
		try {
			reader.read();
		} catch (IOException ignored) {
		}
	}

	private void append(char c) {
		if(index == buffer.length) grow();
		buffer[index++] = c;
	}

	private void grow() {
		buffer = Arrays.copyOf(buffer, buffer.length * 2);
	}
}
