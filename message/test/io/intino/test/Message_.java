package io.intino.test;

import systems.intino.eventsourcing.message.Message;
import systems.intino.eventsourcing.message.MessageReader;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class Message_ {

	@Test
	public void missing_attributes_should_return_false_when_get_as_boolean() {
		Message m = new Message("AB");
		assertFalse(m.get("something").asBoolean());
	}

	@Test
	public void append_to_list() {
		Message m = new Message("A");
		m.set("list", List.of(""));
		assertEquals(new ArrayList<>(), m.get("list").asList(String.class)); // Lists of 1 empty string = empty lists

		m.append("list", "x");
		assertEquals(List.of("x"), m.get("list").asList(String.class));

		m = new Message("A");
		m.set("list", new ArrayList<>());
		assertTrue(m.get("list").asList(String.class).isEmpty());

		m.append("list", "x");
		assertEquals(List.of("x"), m.get("list").asList(String.class));

		m.set("list", "");
		assertEquals(new ArrayList<>(), m.get("list").asList(String.class));

		m.append("list", "x");
		assertEquals(List.of("x"), m.get("list").asList(String.class));
	}

	@Test
	public void lists() {
		Message message = new Message("ABC");
		message.set("list", List.of());
		List<String> list = message.get("list").asList(String.class);
		List<String> list2 = Arrays.asList(message.get("list").as(String[].class));

		System.out.println(message.toString());

		message = new MessageReader(message.toString()).next();
		System.out.println(message.get("list").asList(String.class).size());
	}

	@Test
	public void name() {
		String inl = "[ConsulAssertion]\n" +
				"ts: 2023-03-31T15:05:26.541963Z\n" +
				"ss: MBP-de-Octavio-consul\n" +
				"id: MBP-de-Octavio-consul\n" +
				"version: 1.0.0\n" +
				"installedActivities: io.intino.consul:app-monitor-activity:1.0.0\u0001io.intino.consul:sqlserver-monitor-activity:1.0.0\u0001\n" +
				"enabledActivities: \n" +
				"host: MBP-de-Octavio";

		System.out.println(new MessageReader(inl).next());
	}

	@Test
	public void overrideAttribute() {
		new Message("aaaa").set("nombre", "aaa@aaaa").set("nombre", "aaaa");
	}

	@Test
	public void streams() {
		Message m = new Message("MyType");
		m.set("ints", new int[] {1, 2, 3, 4, 5});
		m.set("email", null); // -> serializes email as a null value

		Optional<String> email = m.get("email").asOptional();

		int[] array1 = m.get("ints").as(int[].class);
		int[] array2 = m.get("ints").orElse(int[].class, new int[0]);

		List<Integer> list = m.get("ints").asList(Integer.class);
		Set<Integer> set = m.get("ints").asSet(Integer.class);
		Queue<Integer> queue = m.get("ints").collect(Integer.class, Collectors.toCollection(ArrayDeque::new));

		String str = m.get("ints").stream(int[].class).map(Arrays::toString).findFirst().orElse("");
		double avg = m.get("ints").flatMap(Integer.class).collect(Collectors.averagingInt(i -> i));

		Message.Value v = m.get("ints");

		assertEquals(List.of(1, 2, 3, 4, 5), v.asList(Integer.class));

		v.flatMap(Integer.class).forEach(System.out::println);
		System.out.println("List => " + v.asList(Long.class));
		System.out.println("Set => " + v.asSet(Long.class));
		System.out.println("Map => " + v.collect(Long.class, Collectors.toMap(i -> i, i -> i * 10)));
		System.out.println("Average => " + v.collect(Long.class, Collectors.averagingLong(i -> i)));
	}

	@Test
	public void nullValues() {
		Integer[] numberList = new Integer[] {1, null, 3, null, null};

		Message m = new Message("something");
		m.set("a", null);
		m.set("b", (List<?>)null);
		m.set("c", numberList);
//		m.set("d", 123);
		m.set("e", "");

		System.out.println(m);

		assertFalse(m.get("a").isEmpty());
		assertNull(m.get("a").asInstant());
		assertNotEquals("", m.get("a").data());
		assertNotEquals("", m.get("a").asString());

		assertFalse(m.get("b").isEmpty());
		assertFalse(m.get("b").asBoolean());

		assertFalse(m.get("c").isEmpty());
		assertArrayEquals(numberList, m.get("c").as(Integer[].class));

		assertTrue(m.get("d").isEmpty());

		assertFalse(m.get("e").isEmpty());
		assertEquals("", m.get("e").asString());
	}

	@Test
	public void iterables() {
		Message m = new Message("something");
		m.set("array", new int[]{1, 2, 3});
		m.set("list", List.of(1, 2, 3));
		m.set("queue", new ArrayDeque<>(List.of(1, 2, 3)));

		assertArrayEquals(new int[]{1, 2, 3}, m.get("array").as(int[].class));
		assertArrayEquals(new int[]{1, 2, 3}, m.get("list").as(int[].class));
		assertArrayEquals(new int[]{1, 2, 3}, m.get("queue").as(int[].class));
	}

	@Test
	public void should_contain_attributes() {
		Message message = new Message("Status")
				.set("battery", 78.0)
				.set("cpuUsage", 11.95)
				.set("isPlugged", true)
				.set("isScreenOn", false)
				.set("temperature", 29.0)
				.set("created", "2017-03-22T12:56:18Z")
				.set("battery", 80.0)
				.set("taps", 100);

		assertThat(message.get("battery").as(Double.class)).isEqualTo(80.0);
		assertThat(message.get("taps").as(Integer.class)).isEqualTo(100);
		assertThat(message.toString()).isEqualTo(
				"[Status]\n" +
						"battery: 80.0\n" +
						"cpuUsage: 11.95\n" +
						"isPlugged: true\n" +
						"isScreenOn: false\n" +
						"temperature: 29.0\n" +
						"created: 2017-03-22T12:56:18Z\n" +
						"taps: 100\n");
	}

	@Test
	public void should_contain_list_attributes() {
		Message message = new Message("Multiline")
				.append("name", "John")
				.append("age", 30)
				.append("age", 20)
				.append("comment", "hello")
				.append("comment", "world")
				.append("comment", "!!!");
		assertThat(message.get("age").toString()).isEqualTo("30\u000120");
		assertThat(message.get("comment").toString()).isEqualTo("hello\u0001world\u0001!!!");
		assertThat(message.toString()).isEqualTo("" +
				"[Multiline]\n" +
				"name: John\n" +
				"age: 30\u000120\n" +
				"comment: hello\u0001world\u0001!!!\n");
	}

	@Test
	public void should_remove_attributes() {
		Message message = new Message("Status")
				.set("battery", 78.0)
				.set("cpuUsage", 11.95)
				.set("isPlugged", true)
				.set("isScreenOn", false)
				.set("temperature", 29.0)
				.set("created", "2017-03-22T12:56:18Z")
				.remove("battery")
				.remove("isscreenon");
		message.remove("isScreenOn");
		assertThat(message.contains("battery")).isEqualTo(false);
		assertThat(message.contains("isScreenOn")).isEqualTo(false);
		assertThat(message.contains("isPlugged")).isEqualTo(true);
	}

	@Test
	public void should_rename_attributes() {
		Message message = new Message("Status")
				.set("battery", 78.0)
				.set("cpuUsage", 11.95)
				.set("isPlugged", true)
				.set("isScreenOn", false)
				.set("temperature", 29.0)
				.set("created", "2017-03-22T12:56:18Z")
				.rename("isPlugged", "plugged")
				.rename("battery", "b");
		assertThat(message.contains("battery")).isEqualTo(false);
		assertThat(message.contains("b")).isEqualTo(true);
		assertThat(message.contains("isPlugged")).isEqualTo(false);
		assertThat(message.contains("plugged")).isEqualTo(true);
	}

	@Test
	public void should_change_type() {
		Message message = new Message("Status");
		message.set("battery", 78.0);
		message.set("cpuUsage", 11.95);
		message.set("isPlugged", true);
		message.set("isScreenOn", false);
		message.set("temperature", 29.0);
		message.set("created", "2017-03-22T12:56:18Z");
//		message.type("sensor");
//		assertThat(message.is("sensor")).isEqualTo(true);
		assertThat(message.contains("battery")).isEqualTo(true);
	}
}
