package systems.intino.eventsourcing.datalake.file.resource;

import io.intino.alexandria.FS;
import io.intino.alexandria.Scale;
import io.intino.alexandria.Timetag;
import systems.intino.eventsourcing.datalake.Datalake;
import systems.intino.eventsourcing.datalake.Datalake.Store.Tub;
import systems.intino.eventsourcing.event.resource.ResourceEvent;

import java.io.File;
import java.util.Optional;
import java.util.stream.Stream;

import static systems.intino.eventsourcing.event.Event.Format.Resource;

public class ResourceEventSource implements Datalake.Store.Source<ResourceEvent> {

	private final File root;

	public ResourceEventSource(File file) {
		this.root = file;
	}

	@Override
	public String name() {
		return root.getName();
	}

	@Override
	public Tub<ResourceEvent> tub(Timetag timetag) {
		File file = new File(root, timetag.value() + Resource.extension());
		return file.exists() ? new ResourceEventTub(file) : null;
	}

	@Override
	public Scale scale() {
		return Optional.ofNullable(first()).map(Tub::scale).orElse(null);
	}

	@Override
	public Tub<ResourceEvent> first() {
		return tubs().findFirst().orElse(null);
	}

	@Override
	public Tub<ResourceEvent> last() {
		File[] files = tubFiles().toArray(File[]::new);
		return files.length == 0 ? null : new ResourceEventTub(files[files.length - 1]);
	}

	@Override
	public Stream<Tub<ResourceEvent>> tubs() {
		return tubFiles().map(ResourceEventTub::new);
	}

	@Override
	public Tub<ResourceEvent> on(Timetag tag) {
		return new ResourceEventTub(new File(root, tag.value() + Resource.extension()));
	}

	private Stream<File> tubFiles() {
		return FS.filesIn(root, pathname -> pathname.getName().endsWith(Resource.extension()));
	}
}
