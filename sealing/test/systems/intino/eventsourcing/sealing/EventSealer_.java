package systems.intino.eventsourcing.sealing;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import systems.intino.eventsourcing.datalake.file.FileDatalake;
import systems.intino.eventsourcing.event.Event;
import systems.intino.eventsourcing.event.EventStream;
import systems.intino.eventsourcing.event.EventWriter;
import systems.intino.eventsourcing.event.message.MessageEvent;
import systems.intino.eventsourcing.event.resource.ResourceEvent;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.shuffle;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EventSealer_ {

	private static final int NUM_FILES_TO_SORT = 20;
	private static final int MAX_NUM_EVENTS_IN_SESSION = 150;
	private static final String[] ResFiles = {"temp/A.txt", "temp/B.txt", "temp/C.txt", "temp/D.docx", "temp/E.pdf", "temp/F.jpg"};
	public static final String MESSAGE_FILE = "temp/datalake/messages/Test/ss/20230327.zim";
	public static final String RESOURCE_FILE = "temp/datalake/resources/Test/ss/20230327.zip";

	@BeforeClass
	public static void createDatalakeSortedFiles() throws IOException {
		createDatalakeFile(MESSAGE_FILE, 100_000, ts -> new MessageEvent("Test", "ss").ts(ts));
		createDatalakeFile(RESOURCE_FILE, 15, ts -> new ResourceEvent("Test", "ss", randomResFile()).ts(ts));
	}

	@Test
	public void sealMessages() throws Exception {
		File destinationFile = new File(MESSAGE_FILE);
		Set<Instant> datalakeEventTsSet = new HashSet<>();
		if(destinationFile.exists()) {
			assertSorted(destinationFile);
			readEventTsSet(destinationFile, datalakeEventTsSet);
		}

		EventSessionSealer sealer = eventSessionSealer();

		createFilesToSort(
				"Test",
				"ss",
				"20230327",
				Event.Format.Message,
				"zim",
				ts -> new MessageEvent("Test", "ss").ts(ts));

		System.out.println("Sealing...");
		sealer.seal();

		assertAllPreviousEventsArePresent(destinationFile, datalakeEventTsSet);
		assertSorted(destinationFile);
		System.out.println("Test ended. All events sealed and sorted in destination");
	}

	@Test
	public void sealResources() throws IOException {
		File destinationFile = new File(RESOURCE_FILE);
		Set<Instant> datalakeEventTsSet = new HashSet<>();
		if(destinationFile.exists()) {
			assertSorted(destinationFile);
			readEventTsSet(destinationFile, datalakeEventTsSet);
		}

		EventSessionSealer sealer = eventSessionSealer();

		createFilesToSort(
				"Test",
				"ss",
				"20230327",
				Event.Format.Resource,
				"zip",
				ts -> new ResourceEvent("Test", "ss", randomResFile()).ts(ts));

		System.out.println("Sealing...");
		sealer.seal();

		assertAllPreviousEventsArePresent(destinationFile, datalakeEventTsSet);
		assertSorted(destinationFile);
		System.out.println("Test ended. All events sealed and sorted in destination");
	}

	private void assertAllPreviousEventsArePresent(File file, Set<Instant> tsSet) throws IOException {
		EventStream.of(file).forEach(e -> tsSet.remove(e.ts()));
		assertTrue(tsSet.isEmpty());
	}

	private void createFilesToSort(String type, String ss, String timetag, Event.Format format, String extension, Function<Instant, Event> eventSupplier) throws IOException {
		System.out.println("Creating files to sort...");
		File stage = new File("temp/stage");
		stage.mkdirs();
		Random random = new Random();
		for(int i = 0;i < NUM_FILES_TO_SORT;i++) {
			int numEvents = random.nextInt(MAX_NUM_EVENTS_IN_SESSION) + 1;
			Instant now = Instant.now();
			List<Instant> ts = IntStream.range(0, numEvents).mapToObj(now::plusSeconds).collect(Collectors.toList());
			shuffle(ts);
			File file = new File(stage, String.format("%s~%s~%s~%s#test-%d.%s.session", type, ss, timetag, format.name(), i, extension));
			try(EventWriter<Event> writer = EventWriter.of(format, new BufferedOutputStream(new FileOutputStream(file)))) {
				for(int j = 0;j < numEvents;j++) {
					Event event = eventSupplier.apply(ts.get(j));
					writer.write(event);
				}
			}
		}
	}

	private void assertSorted(File file) throws IOException {
		Iterator<Event> iterator = EventStream.of(file).iterator();
		Instant ts = null;
		while(iterator.hasNext()) {
			Event event = iterator.next();
			if(ts != null) assertFalse(ts.toEpochMilli() + " is after " + event.ts().toEpochMilli(), ts.isAfter(event.ts()));
			ts = event.ts();
		}
	}

	private void readEventTsSet(File file, Set<Instant> set) throws IOException {
		EventStream.of(file).forEach(e -> set.add(e.ts()));
	}

	private static void createDatalakeFile(String name, int numEvents, Function<Instant, Event> eventSupplier) throws IOException {
		System.out.println("Creating " + name + "...");
		File file = new File(name);
		file.getParentFile().mkdirs();
		Instant ts = Instant.now();
		try(var writer = EventWriter.of(file)) {
			for(int i = 0;i < numEvents;i++) {
				ts = ts.plusSeconds(1);
				writer.write(eventSupplier.apply(ts));
			}
		}
	}

	private static File randomResFile() {
		return new File(ResFiles[new Random().nextInt(ResFiles.length)]);
	}

	private static EventSessionSealer eventSessionSealer() throws IOException {
		File stageDir = new File("temp/stage");
		File tmpDir = new File("temp/stage/temp");

		FileUtils.deleteDirectory(stageDir);
		FileUtils.deleteDirectory(tmpDir);
		stageDir.mkdirs();
		tmpDir.mkdirs();

		return new EventSessionSealer(new FileDatalake(new File("temp/datalake")),
				stageDir,
				tmpDir,
				stageDir);
	}
}
