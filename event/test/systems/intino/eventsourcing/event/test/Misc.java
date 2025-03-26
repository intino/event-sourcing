package systems.intino.eventsourcing.event.test;

import systems.intino.eventsourcing.event.Event;
import systems.intino.eventsourcing.event.EventStream;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class Misc {

	private static AtomicInteger i = new AtomicInteger();
	private static long h = 0;

	public static void main(String[] args) throws IOException {
		while(true) {
			EventStream.of(new File("C:\\Users\\naits\\Desktop\\MonentiaDev\\cinepolis\\temp\\datalake\\events\\ps.Push\\20221030.zim"))
					.peek(k -> i.incrementAndGet())
					.map(Event::ts)
					.reduce((t1, t2) -> t1.plusSeconds(t2.getEpochSecond()))
					.get().getEpochSecond();
		}
	}
}
