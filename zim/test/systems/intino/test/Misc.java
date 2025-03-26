package systems.intino.test;

import systems.intino.eventsourcing.message.Message;
import systems.intino.eventsourcing.zim.ZimStream;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;

public class Misc {

	public static void main(String[] args) throws IOException, InterruptedException {
		while(true) {
			read();
			Thread.sleep(500);
		}
	}

	private static void read() throws IOException {
		Stream<Message> messages = ZimStream.sequence(
				new File("fileformats/zim/test-res/20220727.zim"),
				new File("fileformats/zim/test-res/20220726.zim"),
				new File("fileformats/zim/test-res/20220728.zim"));

		Iterator<Message> iterator = messages.sorted(Comparator.comparing(m -> m.get("ts").asInstant())).iterator();
		Instant lastTs = null;

		while (iterator.hasNext()) {
			Message msg = iterator.next();
			Instant ts = msg.get("ts").asInstant();
			if (lastTs != null && ts.isBefore(lastTs))
				throw new IllegalStateException("Stream is not sorted: " + lastTs + " > " + ts);
			lastTs = ts;
		}
	}
}
