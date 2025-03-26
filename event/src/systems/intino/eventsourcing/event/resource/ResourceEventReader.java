package systems.intino.eventsourcing.event.resource;

import io.intino.alexandria.Resource;
import io.intino.alexandria.resourcecleaner.DisposableResource;
import systems.intino.eventsourcing.event.EventReader;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.stream;

public class ResourceEventReader implements EventReader<ResourceEvent> {
	private final Iterator<ResourceEvent> iterator;

	public ResourceEventReader(File file) throws IOException {
		this(new ResourceToEventIterator(new ZipResourceReader(file)));
	}

	public ResourceEventReader(ResourceEvent... events) {
		this(stream(events));
	}

	public ResourceEventReader(List<ResourceEvent> events) {
		this(events.stream());
	}

	public ResourceEventReader(Stream<ResourceEvent> stream) {
		this(stream.sorted().iterator());
	}

	public ResourceEventReader(Iterator<ResourceEvent> iterator) {
		this.iterator = iterator;
	}

	@Override
	public void close() throws Exception {
		if(iterator instanceof AutoCloseable) ((AutoCloseable) iterator).close();
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public ResourceEvent next() {
		return iterator.next();
	}

	public static class ResourceToEventIterator implements Iterator<ResourceEvent>, AutoCloseable {

		private final Iterator<Resource> iterator;

		public ResourceToEventIterator(Iterator<Resource> iterator) {
			DisposableResource.whenDestroyed(this).thenClose(iterator);
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public ResourceEvent next() {
			return ResourceEvent.of(iterator.next());
		}

		@Override
		public void close() throws Exception {
			if(iterator instanceof AutoCloseable) ((AutoCloseable) iterator).close();
		}
	}
}