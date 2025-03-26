package systems.intino.eventsourcing.sealing;

import io.intino.alexandria.logger.Logger;
import systems.intino.eventsourcing.sealing.SessionSealer.TankNameFilter;
import systems.intino.eventsourcing.sealing.sorters.MessageEventSorter;
import systems.intino.eventsourcing.datalake.Datalake;
import systems.intino.eventsourcing.datalake.file.FileStore;
import systems.intino.eventsourcing.event.Event;
import systems.intino.eventsourcing.event.EventReader;
import systems.intino.eventsourcing.event.EventStream;
import systems.intino.eventsourcing.event.EventWriter;
import systems.intino.eventsourcing.session.Fingerprint;

import java.io.*;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;

public class EventSealer {
	private final Map<Event.Format, Datalake.Store<? extends Event>> stores;
	private final TankNameFilter tankNameFilter;
	private final File tempFolder;
	private boolean multithreading;

	public EventSealer(Datalake datalake, TankNameFilter tankNameFilter, File tempFolder) {
		this(datalake, tankNameFilter, tempFolder, true);
	}

	public EventSealer(Datalake datalake, TankNameFilter tankNameFilter, File tempFolder, boolean multithreading) {
		this.stores = Map.of(Event.Format.Message, datalake.messageStore(), Event.Format.Resource, datalake.resourceStore());
		this.tankNameFilter = requireNonNull(tankNameFilter, "tankNameFilter cannot be null");
		this.tempFolder = requireNonNull(tempFolder, "tempFolder cannot be null");
		this.multithreading = multithreading;
	}

	public EventSealer multithreading(boolean multithreading) {
		this.multithreading = multithreading;
		return this;
	}

	public void seal(Fingerprint fingerprint, List<File> sessions) throws IOException {
		File datalakeFile = datalakeFile(fingerprint);
		if (fingerprint.format().equals(Event.Format.Resource)) sealResources(datalakeFile, sessions);
		else sealMessages(datalakeFile, fingerprint.format(), sort(fingerprint, sessions), temp(datalakeFile));
	}

	private void sealResources(File datalakeFile, List<File> sessions) {
		try (EventWriter<Event> writer = EventWriter.of(datalakeFile)) {
			for (File s : sessions) {
				EventReader<Event> of = EventReader.of(Event.Format.Resource, s);
				of.forEachRemaining(e -> {
					try {
						writer.write(e);
					} catch (IOException ex) {
						Logger.error(ex);
					}
				});
				of.close();
			}
		} catch (Exception e) {
			Logger.error(e);
		}
	}

	private File temp(File datalakeFile) {
		return new File(tempFolder, System.nanoTime() + datalakeFile.getName());
	}

	private void sealMessages(File datalakeFile, Event.Format format, List<File> sortedSessions, File temp) throws
			IOException {
		try {
			try (EventWriter<Event> writer = EventWriter.of(temp)) {
				writer.write(streamOf(format, datalakeFile, sortedSessions));
			}
			Files.move(temp.toPath(), datalakeFile.toPath(), REPLACE_EXISTING, ATOMIC_MOVE);
		} finally {
			temp.delete();
		}
	}

	private Stream<Event> streamOf(Event.Format format, File datalakeFile, List<File> files) {
		return EventStream.merge(Stream.concat(Stream.of(datalakeFile), files.stream()).map(file -> readEvents(format, file)));
	}

	private Stream<Event> readEvents(Event.Format format, File file) {
		if (!file.exists()) return Stream.empty();
		try {
			return readEvents(format, new BufferedInputStream(new FileInputStream(file)));
		} catch (IOException e) {
			Logger.error(e); // TODO
			return Stream.empty();
		}
	}

	private Stream<Event> readEvents(Event.Format format, InputStream inputStream) {
		try {
			return new EventStream<>(readerOf(format, inputStream));
		} catch (IOException e) {
			Logger.error(e); // TODO
			return Stream.empty();
		}
	}

	private EventReader<Event> readerOf(Event.Format type, InputStream inputStream) throws IOException {
		return EventReader.of(type, inputStream);
	}

	private List<File> sort(Fingerprint fingerprint, List<File> files) {
		try {
			EventSorter.Factory sorter = sorterFactoryOf(fingerprint.format());
			if (!tankNameFilter.accepts(fingerprint.tank()) || sorter == null) return files;
			return shouldSortInParallel(files) ? parallelSort(sorter, files) : sequentialSort(sorter, files);
		} catch (Throwable e) {
			Logger.error(e);
			return Collections.emptyList();
		}
	}

	private boolean shouldSortInParallel(List<File> files) {
		return multithreading && files.size() > 1 && Runtime.getRuntime().availableProcessors() > 1;
	}

	private List<File> parallelSort(EventSorter.Factory sorter, List<File> files) throws Throwable {
		ExecutorService threadPool = Executors.newFixedThreadPool(Math.min(4, Runtime.getRuntime().availableProcessors()));
		Throwable[] error = new Throwable[1];
		threadPool.invokeAll(files.stream().map(file -> sort(sorter, file, error)).collect(Collectors.toList()));
		threadPool.shutdown();
		if (error[0] != null) throw error[0];
		return files;
	}

	private Callable<Void> sort(EventSorter.Factory sorter, File file, Throwable[] error) {
		return () -> {
			try {
				sorter.of(file, tempFolder).sort();
			} catch (Throwable e) {
				error[0] = new RuntimeException("Error while sorting " + file + ": " + e.getMessage(), e);
			}
			return null;
		};
	}

	private List<File> sequentialSort(EventSorter.Factory sorter, List<File> files) throws Throwable {
		for (File file : files) sorter.of(file, tempFolder).sort();
		return files;
	}

	public EventSorter.Factory sorterFactoryOf(Event.Format format) {
		switch (format) {
			case Message:
				return MessageEventSorter::new;
//			case Measurement: return new MessageEventSorter(); TODO?
//			case Resource: return ResourceEventSorter::new; TODO?
		}
		return null;
	}

	private File datalakeFile(Fingerprint fingerprint) {
		FileStore store = (FileStore) stores.get(fingerprint.format());
		File datalakeFile = new File(store.directory(), filenameOf(fingerprint) + store.fileExtension());
		datalakeFile.getParentFile().mkdirs();
		return datalakeFile;
	}

	private String filenameOf(Fingerprint fp) {
		return fp.tank() + File.separator + fp.source() + File.separator + fp.timetag();
	}
}
