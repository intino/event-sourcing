package systems.intino.eventsourcing.sealing;

import io.intino.alexandria.Scale;
import io.intino.alexandria.Timetag;
import io.intino.alexandria.logger.Logger;
import org.junit.After;
import org.junit.Test;
import systems.intino.eventsoucing.sealing.FileSessionSealer;
import systems.intino.eventsourcing.datalake.file.FileDatalake;
import systems.intino.eventsourcing.event.Event;
import systems.intino.eventsourcing.event.EventReader;
import systems.intino.eventsourcing.event.message.MessageEvent;
import systems.intino.eventsourcing.ingestion.EventSession;
import systems.intino.eventsourcing.ingestion.SessionHandler;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class SessionManagerTests {

	private final File stageDir = new File("temp/stage");
	private final File datalakeDir = new File("temp/datalake");
	private final File treatedDir = new File("temp/treated");
	private static final File localStageDir = new File("temp/localstage");

	@Test
	public void should_create_a_session() throws IOException {
		SessionHandler handler = new SessionHandler(localStageDir);
		List<MessageEvent> events = createMessageEvents(handler);
		handler.pushTo(stageDir);
		FileSessionSealer fileSessionManager = new FileSessionSealer(new FileDatalake(datalakeDir), stageDir, treatedDir);
		fileSessionManager.seal();
		checkEvents(events);
	}

	private List<MessageEvent> createMessageEvents(SessionHandler handler) throws IOException {
		EventSession eventSession = handler.createEventSession();
		List<MessageEvent> eventList = new ArrayList<>();
		for (int i = 0; i < 30; i++) {
			LocalDateTime now = LocalDateTime.of(2019, 2, 28, 4, 15 + i);
			MessageEvent event = event(now.toInstant(ZoneOffset.UTC), i);
			eventList.add(event);
			eventSession.put("tank1", "test", new Timetag(now, Scale.Hour), Event.Format.Message, new systems.intino.eventsourcing.sealing.Tank1(event));
		}
		eventSession.close();
		return eventList;
	}

	private void checkEvents(List<MessageEvent> eventList) {
		try (EventReader<MessageEvent> reader = EventReader.of(new File("temp/datalake/messages/tank1/test/2019022804.zim"))) {
			for (int i = 0; i < 30; i++) {
				systems.intino.eventsourcing.sealing.Tank1 next = new systems.intino.eventsourcing.sealing.Tank1(reader.next());
				assertEquals(next.ts(), eventList.get(i).ts());
				assertEquals(next.entries(), new systems.intino.eventsourcing.sealing.Tank1(eventList.get(i)).entries());
			}
		} catch (Exception e) {
			Logger.error(e);
		}
	}

	//TODO: merge with existing event files
	private MessageEvent event(Instant instant, int index) {
		return new systems.intino.eventsourcing.sealing.Tank1("test").entries(index).ts(instant);
	}

	@After
	public void tearDown() {
		deleteDirectory(new File("temp"));
	}

	private void deleteDirectory(File directoryToBeDeleted) {
		File[] allContents = directoryToBeDeleted.listFiles();
		if (allContents != null) for (File file : allContents) deleteDirectory(file);
		directoryToBeDeleted.delete();
	}

}
