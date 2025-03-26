package systems.intino.test;

import systems.intino.eventsourcing.message.Message;
import systems.intino.eventsourcing.message.MessageReader;
import systems.intino.eventsourcing.zim.ZimStream;
import systems.intino.eventsourcing.zim.ZimWriter;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Ignore
public class ZimStream_ {

	@Before
	public void setUp() {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
	}

	@Test
	public void misc() throws IOException {
		String inl =
				"[Status]\n" +
						"battery: 78.0\n" +
						"cpuUsage: 11.95\n" +
						"isPlugged: true\n" +
						"isScreenOn: false\n" +
						"temperature: 29.0\n" +
						"created: 2017-03-22T12:56:18Z\n" +
						"\n" +
						"[Status]\n" +
						"battery: 78.0\n" +
						"cpuUsage: 11.95\n" +
						"isPlugged: true\n" +
						"isScreenOn: true\n" +
						"temperature: 10.0\n" +
						"created: 2017-03-22T12:56:18Z\n";

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try(ZimWriter writer = new ZimWriter(out)) {
			writer.write(new MessageReader(inl).next());
		}
		byte[] bytes = out.toByteArray();

		Message message = ZimStream.of(new ByteArrayInputStream(bytes)).findFirst().get();

		try (Stream<Message> of = ZimStream.of(inl).filter(Objects::nonNull)) {
			of.mapToDouble(m -> m.get("temperature").asDouble())
					.reduce(Double::sum)
					.ifPresent(System.out::println);
		}
	}


	@Test
	public void should_read_files() throws IOException {
		try (Stream<Message> messages = ZimStream.sequence(new File("test-res/20220727.zim"), new File("test-res/20220726.zim"), new File("test-res/20220728.zim"))) {

			Iterator<Message> iterator = messages.sorted(Comparator.comparing(m -> m.get("ts").asInstant())).iterator();
			Instant lastTs = null;

			while (iterator.hasNext()) {
				Message msg = iterator.next();
				Instant ts = msg.get("ts").asInstant();
				if (lastTs != null && ts.isBefore(lastTs))
					throw new IllegalStateException("Stream is not sorted: " + lastTs + " > " + ts);
				lastTs = ts;
			}
		}
	}

	@Test
	public void should_read_files_no_try_with_resources() throws IOException {
		Stream<Message> messages = ZimStream.sequence(new File("test-res/20220727.zim"), new File("test-res/20220726.zim"), new File("test-res/20220728.zim"));

		Iterator<Message> iterator = messages.sorted(Comparator.comparing(m -> m.get("ts").asInstant())).iterator();
		Instant lastTs = null;

		while (iterator.hasNext()) {
			Message msg = iterator.next();
			Instant ts = msg.get("ts").asInstant();
			if (lastTs != null && ts.isBefore(lastTs))
				throw new IllegalStateException("Stream is not sorted: " + lastTs + " > " + ts);
			lastTs = ts;
		}
	}

	@Test
	public void should_parse_multiple_messages() {
		String inl =
				"[Status]\n" +
						"battery: 78.0\n" +
						"cpuUsage: 11.95\n" +
						"isPlugged: true\n" +
						"isScreenOn: false\n" +
						"temperature: 29.0\n" +
						"created: 2017-03-22T12:56:18Z\n" +
						"\n" +
						"[Status]\n" +
						"battery: 78.0\n" +
						"cpuUsage: 11.95\n" +
						"isPlugged: true\n" +
						"isScreenOn: true\n" +
						"temperature: 29.0\n" +
						"created: 2017-03-22T12:56:18Z\n";

		ZimStream zimStream = ZimStream.of(inl);
		Message[] messages = new Message[3];
		messages[0] = zimStream.next();
		messages[1] = zimStream.next();
		messages[2] = zimStream.next();
		assertThat(messages[0].is("status")).isTrue();
		assertThat(messages[0].contains("battery")).isTrue();
		assertThat(messages[0].contains("isPlugged")).isTrue();
		assertThat(messages[0].contains("created")).isTrue();
		assertThat(messages[0].contains("xxxx")).isFalse();
		assertThat(messages[0].get("battery").as(Double.class)).isEqualTo(78.0);
		assertThat(messages[0].get("isPlugged").as(Boolean.class)).isTrue();
		assertThat(messages[0].get("created").as(Instant.class).toString()).isEqualTo("2017-03-22T12:56:18Z");
		assertThat(messages[2]).isNull();
	}

	@Test
	public void should_not_ignore_empty_attributes() {
		String inl =
				"[Teacher]\n" +
						"name: Jose\n" +
						"money: 50.0\n" +
						"birthDate: 2016-10-04T20:10:11Z\n" +
						"university: ULPGC\n" +
						"\n" +
						"[Person.Country]\n" +
						"name: Spain\n" +
						"continent:\n";

		Message message = ZimStream.of(inl).next();
		assertThat(message.is("teacher")).isTrue();
		assertThat(message.contains("name")).isTrue();
		assertThat(message.contains("money")).isTrue();
		assertThat(message.contains("birthDate")).isTrue();
		assertThat(message.components("Country").get(0).contains("name")).isTrue();
		assertThat(message.components("Country").get(0).contains("continent")).isTrue();
		assertThat(message.components("Country").get(0).get("name").as(String.class)).isEqualTo("Spain");
		assertThat(message.components("Country").get(0).get("continent").as(String.class)).isEmpty();
	}


	@Test
	public void should_parse_multiline_attributes() {
		String stack =
				"java.lang.NullPointerException: Attempt to invoke interface method 'java.lang.Object java.util.List.get(int)' on a null object reference\n" +
						"    at io.intino.consul.AppService$5.run(AppService.java:154)\n" +
						"    at android.os.Handler.handleCallback(Handler.java:815)\n" +
						"    at android.os.Handler.dispatchMessage(Handler.java:104)\n" +
						"    at android.os.Looper.loop(Looper.java:194)\n" +
						"    at android.app.ActivityThread.main(ActivityThread.java:5666)\n" +
						"    at java.lang.reflect.Method.invoke(Native Method)\n" +
						"    at java.lang.reflect.Method.invoke(Method.java:372)\n" +
						"    at com.android.compiler.os.ZygoteInit$MethodAndArgsCaller.run(ZygoteInit.java:959)\n" +
						"    at com.android.compiler.os.ZygoteInit.main(ZygoteInit.java:754)" +
						"\n" +
						"\n";
		String inl =
				"[Crash]\n" +
						"instant: 2017-03-21T07:39:00Z\n" +
						"app: io.intino.consul\n" +
						"deviceId: b367172b0c6fe726\n" +
						"stack:\n" + indent(stack) + "\n";

		Message message = ZimStream.of(inl).next();
		assertThat(message.type()).isEqualTo("Crash");
		assertThat(message.contains("instant")).isTrue();
		assertThat(message.contains("app")).isTrue();
		assertThat(message.contains("deviceId")).isTrue();
		assertThat(message.contains("stack")).isTrue();
		assertThat(message.get("instant").as(Instant.class).toString()).isEqualTo("2017-03-21T07:39:00Z");
		assertThat(message.get("app").as(String.class)).isEqualTo("io.intino.consul");
		assertThat(message.get("deviceId").as(String.class)).isEqualTo("b367172b0c6fe726");
		assertThat(message.get("stack").as(String.class)).isEqualTo(stack.trim());
	}

	@Test
	public void should_parse_message_with_multiple_components() {
		String inl =
				"[Teacher]\n" +
						"name: Jose\n" +
						"money: 50.0\n" +
						"birthDate: 2016-10-04T20:10:11Z\n" +
						"university: ULPGC\n" +
						"\n" +
						"[Teacher.Country]\n" +
						"name: Spain\n" +
						"\n" +
						"[Teacher.Phone]\n" +
						"value: +150512101402\n" +
						"\n" +
						"[Teacher.Phone.Country]\n" +
						"name: USA\n" +
						"\n" +
						"[Teacher.Phone]\n" +
						"value: +521005101402\n" +
						"\n" +
						"[Teacher.Phone.Country]\n" +
						"name: Mexico\n";

		Message message = ZimStream.of(inl).next();
		assertThat(message.type()).isEqualTo("Teacher");
		assertThat(message.components("country").size()).isEqualTo(1);
		assertThat(message.components("country").get(0).type()).isEqualTo("Country");
		assertThat(message.components("country").get(0).get("name").as(String.class)).isEqualTo("Spain");
		assertThat(message.components("phone").size()).isEqualTo(2);
		assertThat(message.toString()).isEqualTo(inl);
	}

	private static String indent(String text) {
		return "\t" + text.replaceAll("\\n", "\n\t");
	}


}
