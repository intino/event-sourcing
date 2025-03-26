package systems.intino.eventsourcing.sealing;

import io.intino.alexandria.Scale;
import io.intino.alexandria.Timetag;
import systems.intino.eventsourcing.zim.ZimStream;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import systems.intino.eventsourcing.datalake.file.FileDatalake;
import systems.intino.eventsourcing.event.Event;
import systems.intino.eventsourcing.ingestion.EventSession;
import systems.intino.eventsourcing.ingestion.SessionHandler;
import systems.intino.eventsourcing.message.Message;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

@Ignore
public class EventSessionManagerTest {
	private final File stageDir = new File("temp/stage");
	private final File datalakeDir = new File("temp/datalake");
	private final File treatedDir = new File("temp/treated");
	private final File sessionDir = new File("temp/session");

	@Test
	public void should_create_an_event_session() throws IOException {
		SessionHandler handler = new SessionHandler(sessionDir);
		EventSession session = handler.createEventSession();
		List<Message> messageList = new ArrayList<>();
		for (int i = 0; i < 30; i++) {
			LocalDateTime now = LocalDateTime.of(2019, 2, 28, 16, 15 + i);
			Message message = message(now.toInstant(ZoneOffset.UTC), i);
			messageList.add(message);
			session.put("tank1", "test", new Timetag(now, Scale.Hour), Event.Format.Message, new Tank1(message));
		}
		session.close();
		handler.pushTo(stageDir);
		new FileSessionSealer(new FileDatalake(datalakeDir), stageDir, treatedDir).seal();
		ZimStream stream = ZimStream.of(new File("temp/datalake/messages/tank1/test/2019022816.zim"));
		for (int i = 0; i < 30; i++) {
			Message next = stream.next();
			assertEquals(next.get("ts").data(), messageList.get(i).get("ts").data());
			assertEquals(next.get("entries").data(), messageList.get(i).get("entries").data());
		}
	}

	@Test
	public void should_create_an_event_session_without_sorting() throws IOException {
		SessionHandler handler = new SessionHandler(sessionDir);
		EventSession session = handler.createEventSession();
		List<Message> messageList = new ArrayList<>();
		for (int i = 0; i < 30; i++) {
			LocalDateTime now = LocalDateTime.of(2019, 2, 28, 16, 15 + i);
			Message message = message(now.toInstant(ZoneOffset.UTC), i);
			messageList.add(message);
			session.put("tank1", "test", new Timetag(now, Scale.Hour), Event.Format.Message, new Tank1(message));
		}
		session.close();
		handler.pushTo(stageDir);
		new FileSessionSealer(new FileDatalake(datalakeDir), stageDir, treatedDir).seal();
		ZimStream reader = ZimStream.of(new File("temp/datalake/messages/tank1/test/2019022816.zim"));
		for (int i = 0; i < 30; i++) {
			Message next = reader.next();
			assertEquals(next.get("ts").data(), messageList.get(i).get("ts").data());
			assertEquals(next.get("entries").data(), messageList.get(i).get("entries").data());
		}
	}

	//TODO: merge with existing event files
	private Message message(Instant instant, int index) {
		return new Message("tank1").set("ss", "test").set("ts", instant.toString()).set("entries", index);
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