package systems.intino.eventsourcing.datalake.file.resource;

import io.intino.alexandria.FS;
import systems.intino.eventsourcing.datalake.Datalake;
import systems.intino.eventsourcing.event.resource.ResourceEvent;

import java.io.File;
import java.util.stream.Stream;

public class ResourceEventTank implements Datalake.Store.Tank<ResourceEvent> {

	private final File root;

	ResourceEventTank(File root) {
		this.root = root;
	}

	@Override
	public String name() {
		return root.getName();
	}

	public Datalake.Store.Source<ResourceEvent> source(String name) {
		return new ResourceEventSource(new File(root, name));
	}

	@Override
	public Stream<Datalake.Store.Source<ResourceEvent>> sources() {
		return FS.directoriesIn(root).map(ResourceEventSource::new);
	}

	public File root() {
		return root;
	}
}