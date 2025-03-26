package systems.intino.eventsourcing.sealing;

import org.junit.Test;
import systems.intino.eventsoucing.sealing.sorters.MessageEventSorter;
import systems.intino.eventsourcing.event.Event;
import systems.intino.eventsourcing.event.EventStream;
import systems.intino.eventsourcing.event.EventWriter;
import systems.intino.eventsourcing.event.message.MessageEvent;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Random;

import static org.junit.Assert.assertFalse;

public class MessageEventSorterTest {

	@Test
	public void shouldSort() throws Exception {
		File unsortedFile = createUnsortedFile();
		MessageEventSorter sorter = new MessageEventSorter(unsortedFile, new File("temp"));
		File destination = new File("temp/sorted_file.zim");
		if(destination.exists()) destination.delete();
		sorter.sort(destination);
		assertSorted(destination);
	}

	private void assertSorted(File destination) throws Exception {
		Instant last = null;
		var iterator = EventStream.of(destination).iterator();
		while(iterator.hasNext()) {
			Event event = iterator.next();
			if(last != null) assertFalse(last.isAfter(event.ts()));
			last = event.ts();
		}
	}

	private File createUnsortedFile() throws IOException {
		File file = new File("temp/unsorted_messages.zim");
		file.deleteOnExit();
		file.getParentFile().mkdirs();
		try(EventWriter<MessageEvent> writer = EventWriter.of(file)) {
			Random random = new Random();
			int numEvents = 1_000_000;
			int[] offsets = random.ints(numEvents, 1, 1_000_000).toArray();
			Instant now = Instant.now();
			for(int i = 0; i < numEvents; i++) {
				int offset = random.nextBoolean() ? offsets[i] : -offsets[i];
				writer.write(new MessageEvent("Test", "ss").ts(now.plusSeconds(offset)));
			}
		}
		return file;
	}
}
