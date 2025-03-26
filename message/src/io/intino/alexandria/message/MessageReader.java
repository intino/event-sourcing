package systems.intino.eventsourcing.message;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

public class MessageReader implements Iterator<Message>, Iterable<Message>, AutoCloseable {

	private final MessageStream messageStream;
	private final Message[] contextList;
	private List<String> lines;

	public MessageReader(String str) {
		this(new ByteArrayInputStream(str.getBytes()), new Config());
	}

	public MessageReader(String str, Config config) {
		this(new ByteArrayInputStream(str.getBytes()), config);
	}

	public MessageReader(InputStream inputStream) {
		this(new MessageStream(inputStream), new Config());
	}

	public MessageReader(InputStream inputStream, Config config) {
		this(new MessageStream(inputStream), config);
	}

	public MessageReader(MessageStream messageStream) {
		this(messageStream, new Config());
	}

	public MessageReader(MessageStream messageStream, Config config) {
		this.messageStream = messageStream;
		this.contextList = new Message[Math.max(config.contextMaxLevels, 1)];
		init();
	}

	@Override
	public boolean hasNext() {
		return lines != null && lines.size() > 0;
	}

	@Override
	public Message next() {
		try {
			return hasNext() ? nextMessage() : null;
		} catch (MessageException e) {
			throw e;
		} catch (Exception e) {
			throw new MessageException(e.getMessage(), e);
		}
	}

	@Override
	public Iterator<Message> iterator() {
		return this;
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
		parse(lines, contextList);
		while((lines = messageStream.nextLines()).size() > 0 && isComponent(lines.get(0))) {
			parse(lines, contextList);
		}
		return firstNonNullAndReset(contextList);
	}

	private void parse(List<String> lines, Message[] contextList) {
		String type = typeOf(lines.get(0));
		String[] context = type.split("\\.", -1);
		final int level = context.length - 1;
		Message message = new Message(context[level]);
		if(level > 0 && contextList[level - 1] != null) {
			contextList[level - 1].add(message);
		}
		contextList[level] = message;
		readAttributes(message, lines);
	}

	private void readAttributes(Message message, List<String> lines) {
		final int size = lines.size();
		for(int i = 1;i < size;i++) {
			String line = lines.get(i);
			try {
				int attribSep = line.indexOf(':');
				if(attribSep < 0) continue;
				message.setUnsafe(nameOf(line, attribSep), valueOf(line, attribSep));
			} catch (Exception e) {
				throw new MessageException("Malformed message attribute at line " + i + ": " + line, e);
			}
		}
	}

	private String valueOf(String line, int attribSep) {
		int start = line.indexOf(' ', attribSep + 1);
		return line.substring(start < 0 ? attribSep + 1 : start + 1);
	}

	private String nameOf(String line, int attribSep) {
		return line.substring(0, attribSep);
	}

	private String typeOf(String line) {
		return line.substring(1, line.lastIndexOf(']'));
	}

	private boolean isComponent(String next) {
		return next.indexOf('.') > 0;
	}

	private Message firstNonNullAndReset(Message[] messages) {
		Message message = null;
		for(int i = 0; i < messages.length; i++) {
			Message m = messages[i];
			if (m != null && message == null) {
				message = m;
			}
			messages[i] = null;
		}
		if(message == null) throw new NoSuchElementException("No messages left");
		return message;
	}

	private void init() {
		if (this.messageStream.hasNext())
			this.lines = this.messageStream.nextLines();
	}

	public static class Config {
		private int linesBufferSize = 128;
		private int contextMaxLevels = 8;

		public Config linesBufferSize(int linesBufferSize) {
			this.linesBufferSize = linesBufferSize;
			return this;
		}

		public Config contextMaxLevels(int contextMaxLevels) {
			this.contextMaxLevels = contextMaxLevels;
			return this;
		}
	}
}