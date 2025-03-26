package io.intino.performance.impl;

import systems.intino.eventsourcing.message.Message;
import systems.intino.eventsourcing.message.MessageException;
import systems.intino.eventsourcing.message.MessageStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

public class FastMessageReaderOld implements Iterator<Message>, Iterable<Message>, AutoCloseable {
	private final MessageStream messageStream;
	private String currentStr;

	public FastMessageReaderOld(String str) {
		this(new ByteArrayInputStream(str.getBytes()));
	}

	public FastMessageReaderOld(InputStream inputStream) {
		this(new MessageStream(inputStream));
	}

	public FastMessageReaderOld(MessageStream messageStream) {
		this.messageStream = messageStream;
		if (this.messageStream.hasNext()) currentStr = this.messageStream.next();
	}

	public boolean hasNext() {
		return currentStr != null && !currentStr.isEmpty() && !currentStr.isBlank();
	}

	public Message next() {
		try {
			return hasNext() ? nextMessage() : null;
		} catch (Exception e) {
			throw new MessageException(e.getMessage(),e);
		}
	}

	@Override
	public void close() throws Exception {
		try {
			messageStream.close();
		} catch (IOException e) {
			Logger.getGlobal().severe(e.getMessage());
		}
	}

	private Message nextMessage() {
		Message[] messages = new Message[16];
		parse(currentStr, messages);
		String nextStr;
		while((nextStr = messageStream.next()) != null && isComponent(nextStr)) {
			parse(nextStr, messages);
		}
		currentStr = nextStr;
		return firstNonNull(messages);
	}

	private Message firstNonNull(Message[] messages) {
		for(Message m : messages) if(m != null) return m;
		throw new NoSuchElementException();
	}

	private void parse(String msgStr, Message[] contextList) {
		String[] lines = msgStr.split("\n", -1);
		String type = typeOf(lines[0]);
		String[] context = type.split("\\.", -1);
		int level = context.length - 1;
		Message message = new Message(context[level]);
		if(level > 0 && contextList[level - 1] != null) {
			contextList[level - 1].add(message);
		}
		contextList[level] = message;
		readAttributes(message, lines);
	}

	private void readAttributes(Message message, String[] lines) {
		for(int i = 1;i < lines.length;i++) {
			String line = lines[i];
			if(line.isBlank()) continue;
			int attribSep = line.indexOf(':');
			String name = line.substring(0, attribSep);
			String value = line.substring(attribSep + 1).trim();
			if(!value.isEmpty()) {
				message.set(name, value);
				continue;
			}
			i = readMultilineAttribute(message, lines, i, name);
		}
	}

	private static int readMultilineAttribute(Message message, String[] lines, int i, String name) {
		String line;
		StringBuilder multilineValue = new StringBuilder(128);
		for(i = i + 1; i < lines.length; i++) {
			line = lines[i];
			if(!line.startsWith("\t")) {
				multilineValue.setLength(Math.max(0, multilineValue.length() - 1));
				message.set(name, multilineValue.toString());
				break;
			}
			multilineValue.append(line.substring(1)).append('\n');
		}
		return i;
	}

	private String typeOf(String line) {
		return line.substring(1, line.lastIndexOf(']'));
	}

	private void add(List<Message> messageContexts, int level, Message value) {
		if (messageContexts.size() <= level) messageContexts.add(level, value);
		else messageContexts.set(level, value);
	}

	private boolean isComponent(String next) {
		final int componentSep = next.indexOf('.');
		return componentSep > 0 && componentSep < next.indexOf('\n');
	}

	@Override
	public Iterator<Message> iterator() {
		return this;
	}
}