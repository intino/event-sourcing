package systems.intino.eventsourcing.event.resource;

import io.intino.alexandria.Resource;
import io.intino.alexandria.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static systems.intino.eventsourcing.event.resource.ResourceEvent.METADATA;
import static systems.intino.eventsourcing.event.resource.ResourceEvent.REI.ID_SEP;
import static systems.intino.eventsourcing.event.resource.ResourceHelper.METADATA_FILE;

public class ZipResourceReader implements Iterator<Resource>, AutoCloseable {
	private final File file;
	private final ZipFile zipFile;
	private final Map<Object, ZipEntry> entries;
	private final Iterator<? extends ZipEntry> iterator;

	public ZipResourceReader(File file) throws IOException {
		this.file = file;
		zipFile = new ZipFile(file);
		entries = Collections.list(zipFile.entries()).stream().collect(Collectors.toMap(ZipEntry::getName, e -> e));
		iterator = entries.values().stream().filter(e -> !e.getName().endsWith(METADATA)).sorted(Comparator.comparing(ZipEntry::getName)).iterator();
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public Resource next() {
		return toResource(iterator.next());
	}

	private Resource toResource(ZipEntry entry) {
		Map<String, String> metadata = deserializeMetadata(entry);
		String name = entry.getName().substring(entry.getName().indexOf(ID_SEP) + 1).replace("$", "/");
		Resource resource = new Resource(name, inputStreamProviderOf(entry));
		resource.metadata().putAll(metadata);
		return resource;
	}

	private Map<String, String> deserializeMetadata(ZipEntry entry) {
		ZipEntry metadataEntry = entries.get(entry.getName() + METADATA);
		if (metadataEntry != null) {
			try {
				Map<String, String> metadata = ResourceHelper.deserializeMetadata(new String(zipFile.getInputStream(metadataEntry).readAllBytes(), StandardCharsets.UTF_8));
				if (metadata == null) return new HashMap<>();
				if (file != null) metadata.put(METADATA_FILE, file.getAbsolutePath());
				return metadata;
			} catch (IOException e) {
				Logger.error(e);
			}
		}
		return new HashMap<>();
	}

	private Resource.InputStreamProvider inputStreamProviderOf(ZipEntry entry) {
		return () -> {
			ZipFile zip = new ZipFile(file);
			return zip.getInputStream(zip.getEntry(entry.getName()));
		};
	}

	@Override
	public void close() throws Exception {
		zipFile.close();
	}
}
