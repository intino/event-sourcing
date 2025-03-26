package systems.intino.eventsourcing.datalake.file;

import java.io.File;

public interface FileStore {
	String fileExtension();

	File directory();
}
