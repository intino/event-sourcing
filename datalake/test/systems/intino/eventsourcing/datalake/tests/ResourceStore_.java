package systems.intino.eventsourcing.datalake.tests;

import systems.intino.eventsourcing.datalake.Datalake;
import systems.intino.eventsourcing.datalake.file.FileDatalake;
import systems.intino.eventsourcing.event.EventWriter;
import systems.intino.eventsourcing.event.resource.ResourceEvent;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class ResourceStore_ {

	public static void main(String[] args) throws IOException {
//		test1();
		test2();
	}

	private static void test2() {
		Datalake datalake = new FileDatalake(new File("temp/datalake"));
		Datalake.ResourceStore resources = datalake.resourceStore();
//		C:\Users\naits\AppData\Local\Temp\Rar$DRa5736.38091\1695802200183#$var$log$intino$data-hub.log
		//Log#45-79-45-227-ip-linodeusercontent-com.com.cinepolis:data-hub#1695802200183#/var/log/intino/data-hub.log
		var event = resources.find(ResourceEvent.REI.of("Log#45-79-45-227-ip-linodeusercontent-com.com.cinepolis_data-hub#1695802200183#/var/log/intino/data-hub.log"));
		System.out.println(event);
	}

	private static void test1() throws IOException {
		writeResources();

		Datalake datalake = new FileDatalake(new File("temp/datalake"));
		Datalake.ResourceStore resources = datalake.resourceStore();

		// List all the events in this store. At this point, NO resources are loaded into memory and no files are opened
		List<ResourceEvent> events = resources.content().collect(Collectors.toList());
		events.forEach(System.out::println);

		// Read resource
		ResourceEvent event = events.get(0);
		try(InputStream inputStream = event.resource().open()) {
			// Open the resource on demand
			System.out.println("\n");
		}

		int[] hashes = new int[events.size() * 10];
		var threadPool = Executors.newCachedThreadPool();
		for(int i = 0;i < hashes.length;i++) {
			var e = events.get(i % events.size());
			int index = i;
			threadPool.execute(() -> {
				try(InputStream inputStream = e.resource().open()) {
					// Open the resource on demand
					hashes[index] = inputStream.readAllBytes().hashCode();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			});
		}
		waitFor(threadPool);

		System.out.println(Arrays.toString(hashes));

		// Find directly by REI (Resource Event Identifier)
		//Optional<ResourceEvent> result = resources.find("<type>/<ss>/<ts>/<resource-name>");
		Optional<ResourceEvent> result = resources.find(event.getREI());

		if(result.isEmpty()) return;

		System.out.println("findByREI = " + result.get().resource().safeReader().ofEmpty().readAsString());
	}

	private static void waitFor(ExecutorService threadPool) {
		threadPool.shutdown();
		try {
			threadPool.awaitTermination(5, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private static void writeResources() throws IOException {
		LocalDateTime ts = LocalDateTime.of(2023, 1, 1, 12, 0);
		List<ResourceEvent> events = List.of(
				new ResourceEvent("Log", "ss", new File("temp/C.txt")).ts(ts.plusMinutes(50).toInstant(ZoneOffset.UTC)),
				new ResourceEvent("Log", "ss", new File("temp/B.txt")).ts(ts.plusMinutes(30).toInstant(ZoneOffset.UTC)),
				new ResourceEvent("Log", "ss", new File("temp/A.txt")).ts(ts.plusMinutes(10).toInstant(ZoneOffset.UTC)),
				new ResourceEvent("Log", "ss", new File("temp/A.txt")).ts(ts.plusMinutes(0).toInstant(ZoneOffset.UTC))
		);

		File session = new File("temp/resources_20230101_session.zip");
		session.getParentFile().mkdirs();
		EventWriter.write(session, events.stream());

		File file = new File("temp/datalake/resources/Log/ss/20230101.zip");
		file.getParentFile().mkdirs();
		Files.move(session.toPath(), file.toPath(), REPLACE_EXISTING);
	}
}
