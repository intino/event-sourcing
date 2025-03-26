package systems.intino.eventsourcing.datalake.file.resource;

import io.intino.alexandria.FS;
import io.intino.alexandria.Scale;
import io.intino.alexandria.Timetag;
import io.intino.alexandria.logger.Logger;
import systems.intino.eventsourcing.datalake.Datalake;
import systems.intino.eventsourcing.datalake.file.FileStore;
import systems.intino.eventsourcing.event.Event;
import systems.intino.eventsourcing.event.resource.ResourceEvent;

import java.io.File;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class ResourceEventStore implements Datalake.ResourceStore, FileStore {

	private final File root;

	public ResourceEventStore(File root) {
		this.root = root;
	}

	@Override
	public Optional<ResourceEvent> find(ResourceEvent.REI rei) {
		try {
			Scale scale = scale();
			if(scale == null) return Optional.empty();
			Timetag timetag = Timetag.of(rei.ts(), scale);
			return findResourceEvent(rei, timetag);
		} catch (Exception e) {
			Logger.error(e);
			return Optional.empty();
		}
	}

	private Optional<ResourceEvent> findResourceEvent(ResourceEvent.REI rei, Timetag timetag) {
		File tub = new File(root, rei.type() + "/" + rei.ss() + "/" + timetag + fileExtension());
		if(tub.exists()) return new ResourceEventTub(tub).find(rei);
		return findEventInTanks(rei, timetag);
	}

	private Optional<ResourceEvent> findEventInTanks(ResourceEvent.REI rei, Timetag timetag) {
		return tanks()
				.filter(t -> t.name().endsWith(rei.type()))
				.filter(t -> t.source(rei.ss()) != null)
				.map(t -> t.source(rei.ss()).tub(timetag))
				.filter(Objects::nonNull)
				.filter(t -> t instanceof ResourceEventTub)
				.map(t -> ((ResourceEventTub) t).find(rei))
				.filter(Optional::isPresent)
				.findFirst().orElse(Optional.empty());
	}

	@Override
	public Stream<Tank<ResourceEvent>> tanks() {
		return FS.directoriesIn(root).map(ResourceEventTank::new);
	}

	public File directory() {
		return root;
	}

	@Override
	public ResourceEventTank tank(String name) {
		return new ResourceEventTank(new File(root, name));
	}

	@Override
	public String fileExtension() {
		return Event.Format.Resource.extension();
	}
}
