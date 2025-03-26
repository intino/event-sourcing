package systems.intino.eventsourcing.datalake.tests;

import systems.intino.eventsourcing.event.EventStream;
import systems.intino.eventsourcing.event.message.MessageEvent;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.junit.Assert.assertFalse;

public class EventStreamTest {
	private static final Instant NOW = LocalDateTime.of(2023, 1, 1, 0, 0).toInstant(ZoneOffset.UTC);

	@Test
	public void merge() {
		Stream<E> a = E.generate(2, 10, 14, 22);
		Stream<E> b = E.generate(3, 13, 20);
		Stream<E> c = E.generate();
		Stream<E> d = E.generate(1, 23);

		Instant last = null;
		Iterator<E> iterator = EventStream.merge(Stream.of(a, b, c, d)).iterator();
		while(iterator.hasNext()) {
			E e = iterator.next();
			if(last != null) assertFalse(last.isAfter(e.ts()));
			last = e.ts();
		}
	}

	private static class E extends MessageEvent {

		public static Stream<E> generate(int... offsets) {
			return Arrays.stream(offsets).mapToObj(E::new);
		}

		public E(int offset) {
			super("E", "ss");
			ts(NOW.plusSeconds(offset));
		}
	}
}
