package systems.intino.eventsourcing.datalake;

import java.io.File;

public interface FileTub {
	String fileExtension();

	File file();
}
