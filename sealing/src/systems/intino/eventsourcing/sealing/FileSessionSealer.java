package systems.intino.eventsourcing.sealing;

import io.intino.alexandria.logger.Logger;
import systems.intino.eventsourcing.datalake.file.FileDatalake;

import java.io.File;

public class FileSessionSealer implements SessionSealer {
	private final File stageDir;
	private final FileDatalake datalake;
	private final File tempDir;
	private final File treatedDir;

	public FileSessionSealer(FileDatalake datalake, File stageDir, File treatedDir) {
		this(datalake, stageDir, tempFolder(stageDir), treatedDir);
	}

	public FileSessionSealer(FileDatalake datalake, File stageDir, File tempDir, File treatedDir) {
		this.datalake = datalake;
		this.stageDir = stageDir;
		this.tempDir = tempDir;
		this.treatedDir = treatedDir;
	}

	@Override
	public synchronized void seal(TankFilter tankFilter) {
		try {
			sealEvents(tankFilter);
		} catch (Throwable e) {
			Logger.error(e);
		}
	}

	private void sealEvents(TankFilter tankFilter) {
		new EventSessionSealer(datalake, stageDir, tempDir, treatedDir).seal(t -> check(t, tankFilter));
	}

	private boolean check(String tank, TankFilter tankFilter) {
		return tankFilter.accepts(datalake.messageStore().tank(tank))
			   || tankFilter.accepts(datalake.resourceStore().tank(tank));
	}

	private static File tempFolder(File stageFolder) {
		File temp = new File(stageFolder, "temp");
		temp.mkdir();
		return temp;
	}
}