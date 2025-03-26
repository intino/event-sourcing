package systems.intino.eventsourcing.event.test;

import systems.intino.eventsourcing.event.EventStream;
import systems.intino.eventsourcing.event.EventWriter;
import systems.intino.eventsourcing.event.message.MessageEvent;
import systems.intino.eventsourcing.event.message.MessageEventWriter;
import systems.intino.eventsourcing.message.Message;
import systems.intino.eventsourcing.message.MessageReader;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class MessageEvent_ {

	private static final List<Message> Ref = loadRefFile();

	@Test
	public void write_events_into_zim_from_scratch() throws IOException {
		File file = new File("../temp/zim_from_scratch.zim");
		file.delete();
		file.getParentFile().mkdirs();
		try(EventWriter<MessageEvent> writer = new MessageEventWriter(file)) {
			writer.write(Ref.stream().map(MessageEvent::new));
		}
		compareWithRef(file);
	}

	@Test
	public void append_events_into_an_existing_zim() throws IOException {
		List<Message> messagesAlreadyWritten = Ref.subList(0, Ref.size() / 2);
		File file = new File("../temp/append_zim.zim");
		file.delete();
		file.getParentFile().mkdirs();
		setupZimWithSomeEventsInIt(file, messagesAlreadyWritten);
		try(EventWriter<MessageEvent> writer = new MessageEventWriter(file)) {
			writer.write(Ref.stream().skip(messagesAlreadyWritten.size()).map(MessageEvent::new));
		}
		compareWithRef(file);
	}

	private void setupZimWithSomeEventsInIt(File file, List<Message> messages) throws IOException {
		EventWriter.write(file, messages.stream().map(MessageEvent::new));
	}

	private void compareWithRef(File file) throws IOException {
		List<MessageEvent> events = EventStream.<MessageEvent>of(file).collect(Collectors.toList());
		assertEquals(Ref.size(), events.size());

		for(int i = 0;i < events.size();i++) {
			assertEquals(Ref.get(i), events.get(i).toMessage());
		}
	}

	private static List<Message> loadRefFile() {
		List<Message> messages = new ArrayList<>();
		try(MessageReader reader = new MessageReader(MessageEvent_.class.getResourceAsStream("/events.inl"))) {
			while(reader.hasNext()) messages.add(reader.next());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return messages;
	}
}
