package systems.intino.eventsourcing.ingestion;

import io.intino.alexandria.FS;
import systems.intino.eventsourcing.event.Event.Format;
import io.intino.alexandria.logger.Logger;
import systems.intino.eventsourcing.session.Fingerprint;
import systems.intino.eventsourcing.session.Session;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.UUID.randomUUID;
import static systems.intino.eventsourcing.session.Session.SessionExtension;

public class SessionHandler {
	private final File root;
	private final List<PrivateSession> sessions = new ArrayList<>();

	public SessionHandler() {
		this.root = null;
	}

	public SessionHandler(File root) {
		this.root = root;
		this.root.mkdirs();
		this.sessions.addAll(loadFileSessions());
	}

	public EventSession createEventSession() {
		return new EventSession(new PrivateProvider());
	}

	public EventSession createEventSession(int autoFlushSize) {
		return new EventSession(new PrivateProvider(), autoFlushSize);
	}

	public void pushTo(File stageFolder) {
		File destination = new File(stageFolder, root.getName());
		destination.mkdirs();
		sessions().forEach(s -> push(s, destination));
	}

	public void clear() {
		sessions.stream().filter(s -> s.data() instanceof FileSessionData).forEach(s -> ((FileSessionData) s.data()).file().delete());
		sessions.clear();
	}

	public Stream<Session> sessions() {
		return sessions.stream()
				.map(s -> new Session() {
					@Override
					public String name() {
						return s.name();
					}

					@Override
					public Format format() {
						return s.format();
					}

					@Override
					public InputStream inputStream() {
						return s.inputStream();
					}
				});
	}

	private List<PrivateSession> loadFileSessions() {
		return sessionFiles()
				.map(f -> new PrivateSession(name(f), format(f), new FileSessionData(f))).collect(Collectors.toList());
	}

	private Stream<File> sessionFiles() {
		try {
			return this.root == null ? Stream.empty() : FS.allFilesIn(root, path -> path.getName().endsWith(SessionExtension));
		} catch (IOException e) {
			Logger.error(e); // TODO
			return Stream.empty();
		}
	}

	private String name(File f) {
		return f.getName().substring(0, f.getName().indexOf(format(f).name()) - 1);
	}

	private Format format(File f) {
		return Fingerprint.of(f).format();
	}

	private void push(Session session, File stageFolder) {
		try {
			FS.copyInto(fileFor(session, stageFolder), session.inputStream());
		} catch (IOException e) {
			Logger.error(e); // TODO
		}
	}

	private File fileFor(Session session, File stageFolder) {
		return new File(stageFolder, filename(session));
	}

	private String filename(Session session) {
		return session.name() + SessionExtension;
	}

	private interface SessionData {
		InputStream inputStream();

		OutputStream outputStream();

		File outputFile();
	}

	public interface Provider {
		OutputStream outputStream(Format format);

		OutputStream outputStream(String name, Format format);

		File file(String name, Format format);
	}

	private static class FileSessionData implements SessionData {
		private final File file;

		FileSessionData(File file) {
			this.file = file;
		}

		@Override
		public InputStream inputStream() {
			try {
				return new FileInputStream(file);
			} catch (FileNotFoundException e) {
				Logger.error(e);
				return null;
			}
		}

		public File file() {
			return file;
		}

		@Override
		public OutputStream outputStream() {
			try {
				if (!file.exists()) file.createNewFile();
				return new FileOutputStream(file);
			} catch (IOException e) {
				Logger.error(e);
				return null;
			}
		}

		@Override
		public File outputFile() {
			return file;
		}
	}

	private static class MemorySessionData implements SessionData {

		private final ByteArrayOutputStream outputStream;

		public MemorySessionData() {
			this.outputStream = new ByteArrayOutputStream();
		}

		@Override
		public InputStream inputStream() {
			return new ByteArrayInputStream(outputStream.toByteArray());
		}

		@Override
		public OutputStream outputStream() {
			return outputStream;
		}

		@Override
		public File outputFile() {
			return null;
		}
	}

	private static class PrivateSession {
		private final String name;
		private final Format format;
		private final SessionData sessionData;

		PrivateSession(String name, Format format, SessionData sessionData) {
			this.name = name;
			this.format = format;
			this.sessionData = sessionData;
		}

		public String name() {
			return name;
		}

		public Format format() {
			return format;
		}

		SessionData data() {
			return sessionData;
		}

		InputStream inputStream() {
			return sessionData.inputStream();
		}

		OutputStream outputStream() {
			return sessionData.outputStream();
		}

		public File file() {
			return sessionData.outputFile();
		}
	}

	private class PrivateProvider implements Provider {

		public OutputStream outputStream(Format format) {
			return outputStream("", format);
		}

		public OutputStream outputStream(String name, Format format) {
			PrivateSession session = session(name + suffix(), format);
			sessions.add(session);
			return session.outputStream();
		}

		@Override
		public File file(String name, Format format) {
			PrivateSession session = session(name + suffix(), format);
			sessions.add(session);
			return session.file();
		}

		private PrivateSession session(String name, Format type) {
			return new PrivateSession(name, type, root == null ? new MemorySessionData() : new FileSessionData(fileOf(name)));
		}

		private File fileOf(String name) {
			return new File(root, filename(name));
		}

		private String filename(String name) {
			return name + SessionExtension;
		}

		private String suffix() {
			return "#" + randomUUID();
		}
	}
}