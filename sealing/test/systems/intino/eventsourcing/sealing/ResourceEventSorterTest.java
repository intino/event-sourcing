package systems.intino.eventsourcing.sealing;

import io.intino.alexandria.Resource;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import systems.intino.eventsoucing.sealing.sorters.ResourceEventSorter;
import systems.intino.eventsourcing.event.EventReader;
import systems.intino.eventsourcing.event.EventWriter;
import systems.intino.eventsourcing.event.resource.ResourceEvent;
import systems.intino.eventsourcing.event.resource.ResourceEventReader;
import systems.intino.eventsourcing.event.resource.ResourceEventWriter;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Ignore
public class ResourceEventSorterTest {

	private static final File THE_ZIP_FILE = new File("temp/sorter_test.zip");
	private static final int NUM_EVENTS = 100000;

	@BeforeClass
	public static void createZipFile() {
		THE_ZIP_FILE.delete();
		Random random = new Random(System.currentTimeMillis());
		int[] offsets = random.ints(NUM_EVENTS, 0, 100000).toArray();
		Instant ts = Instant.now();
		byte[] bytes = new byte[8 * 1024];
		try(EventWriter<ResourceEvent> writer = new ResourceEventWriter(THE_ZIP_FILE)) {
			for(int i = 0;i < NUM_EVENTS;i++) {
				random.nextBytes(bytes);
				ResourceEvent event = new ResourceEvent("Test", "test", new Resource("res" + i, bytes));
				writer.write(event.ts(ts.plusSeconds(random.nextBoolean() ? offsets[i] : -offsets[i])));
				if((i+1) % 1000 == 0) System.out.println("Written " + (i+1) + " resources");
			}
			System.out.println("Zip file created!");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void sort() throws Exception {
		File temp = THE_ZIP_FILE.getParentFile();
		File destination = new File(THE_ZIP_FILE.getAbsolutePath().replace(".zip", ".sorted.zip"));
		destination.delete();

		long start = System.currentTimeMillis();
		new ResourceEventSorter(THE_ZIP_FILE, temp).sort(destination);
		float time = (System.currentTimeMillis() - start) / 1000.0f;
		System.out.println("Sorted in " + (time) + " seconds");

		Instant last = null;
		int count = 0;
		try(EventReader<ResourceEvent> reader = new ResourceEventReader(destination)) {
			while(reader.hasNext()) {
				ResourceEvent event = reader.next();
				if(last != null) assertFalse(last.isAfter(event.ts()));
				last = event.ts();
				++count;
			}
		}
		assertEquals(NUM_EVENTS, count);
	}
}
