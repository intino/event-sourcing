package systems.intino.eventsourcing.ingestion;

import io.intino.alexandria.Timetag;
import systems.intino.eventsourcing.event.Event;
import systems.intino.eventsourcing.event.Event.Format;
import systems.intino.eventsourcing.event.EventWriter;
import io.intino.alexandria.logger.Logger;
import systems.intino.eventsourcing.session.Fingerprint;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class EventSession {
	private final Map<Fingerprint, EventWriter<Event>> writers = new ConcurrentHashMap<>();
	private final SessionHandler.Provider provider;
	private final int autoFlush;
	private final AtomicInteger count = new AtomicInteger();

	public EventSession(SessionHandler.Provider provider) {
		this(provider, 1_000_000);
	}

	public EventSession(SessionHandler.Provider provider, int autoFlush) {
		this.provider = provider;
		this.autoFlush = autoFlush;
	}

	public void put(String tank, String source, Timetag timetag, Format format, Event... events) throws IOException {
		put(tank, source, timetag, format, Arrays.stream(events));
		if (count.addAndGet(events.length) >= autoFlush) flush();
	}

	public void put(String tank, String source, Timetag timetag, Format format, Stream<Event> eventStream) throws IOException {
		put(writerOf(tank, source, timetag, format), eventStream);
	}

	public void flush() {
		for (EventWriter<? extends Event> w : writers.values())
			synchronized (w) {
				try {
					w.flush();
				} catch (IOException e) {
					Logger.error(e);
				}
			}
		count.set(0);
	}

	public void close() {
		try {
			for (EventWriter<? extends Event> w : writers.values())
				synchronized (w) {
					w.close();
				}
		} catch (IOException e) {
			Logger.error(e);
		}
	}

	private void put(EventWriter<Event> writer, Stream<Event> events) {
		synchronized (writer) {
			events.forEach(e -> write(writer, e));
		}
	}

	private void write(EventWriter<Event> writer, Event event) {
		try {
			writer.write(event);
		} catch (IOException e) {
			Logger.error(e);
		}
	}

	private EventWriter<Event> writerOf(String tank, String source, Timetag timetag, Format format) throws IOException {
		return writerOf(Fingerprint.of(tank, withOutParameters(source), timetag, format));
	}

	private EventWriter<Event> writerOf(Fingerprint fingerprint) throws IOException {
		synchronized (writers) {
			if (!writers.containsKey(fingerprint)) writers.put(fingerprint, createWriter(fingerprint));
			return writers.get(fingerprint);
		}
	}

	private EventWriter<Event> createWriter(Fingerprint fingerprint) throws IOException {
		return EventWriter.of(fingerprint.format(), provider.file(fingerprint.name(), fingerprint.format()));
	}

	private static String withOutParameters(String ss) {
		return ss.contains("?") ? ss.substring(0, ss.indexOf("?")) : ss;
	}
}