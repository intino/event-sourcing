package systems.intino.eventsourcing.event.resource;

import systems.intino.eventsourcing.event.EventWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Map;

import static systems.intino.eventsourcing.event.resource.ResourceEvent.ENTRY_NAME_SEP;
import static systems.intino.eventsourcing.event.resource.ResourceEvent.METADATA;
import static systems.intino.eventsourcing.event.resource.ResourceHelper.serializeMetadata;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class ResourceEventWriter implements EventWriter<ResourceEvent> {

	private final File file;

	public ResourceEventWriter(File file) {
		this.file = file;
	}

	@Override
	public void write(ResourceEvent event) throws IOException {
		URI uri = URI.create("jar:" + file.toPath().toUri());
		try (FileSystem fs = FileSystems.newFileSystem(uri, Map.of("create", "true")); InputStream data = event.resource().stream()) {
			addData(fs, event.getREI(), data);
			addMetaData(fs, event.getREI(), serializeMetadata(event, file));
		}
	}

	private static void addData(FileSystem fs, ResourceEvent.REI rei, InputStream data) throws IOException {
		Files.write(fs.getPath(normalizePath(rei)), data.readAllBytes(), CREATE, APPEND);
	}

	private static void addMetaData(FileSystem fs, ResourceEvent.REI rei, String metadata) throws IOException {
		String entryMetadataName = normalizePath(rei) + METADATA;
		Files.writeString(fs.getPath(entryMetadataName), metadata, CREATE, APPEND);
	}

	private static String normalizePath(ResourceEvent.REI rei) {
		return rei.resourceId().replace("/", ENTRY_NAME_SEP).replace("\\", ENTRY_NAME_SEP);
	}

	@Override
	public void flush() {
	}

	@Override
	public void close() throws IOException {
	}
}
