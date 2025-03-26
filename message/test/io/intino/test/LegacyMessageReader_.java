package io.intino.test;

import systems.intino.eventsourcing.message.LegacyMessageReader;
import systems.intino.eventsourcing.message.MessageException;
import systems.intino.eventsourcing.message.MessageReader;
import systems.intino.eventsourcing.message.Message;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LegacyMessageReader_ {

	@Test
	public void sould_ignore_lines_without_colon_or_multiline_indentation() {
		String inl = "[Something]\n" +
				"ts: 2018-01-01T00:00:16Z\n" +
				"attribWithLineBreaks: this\n" +
				"is not\n" + // This line should be ignored
				"a multiline attrib\n" + // This line should be ignored
				"\n" + // This line should be ignored
				"cuenta: ABC\n" +
				"ss: default";

		System.out.println(inl);

		Message m = new LegacyMessageReader(inl).next();
		assertNotNull(m);

		String[] attribWithLineBreaks = m.get("attribWithLineBreaks").asMultiline();

		assertEquals(4, m.attributes().size());

		assertEquals("2018-01-01T00:00:16Z", m.get("ts").asString());
		assertEquals("this", m.get("attribWithLineBreaks").asString());
		assertEquals("ABC", m.get("cuenta").asString());
		assertEquals("default", m.get("ss").asString());
	}

	@Test
	public void sould_read_message_with_attributes_after_multiline_attribute() {
		String inl = "[CuentaDestinatarios]\n" +
				"ts: 2018-01-01T00:00:16Z\n" +
				"emails:\n" +
				"\tsomeemail@gmail.com\n" +
				"\tother_email@outlook.es\n" +
				"cuenta: ABC\n" +
				"ss: default";

		Message m = new LegacyMessageReader(inl).next();
		assertNotNull(m);

		assertEquals(4, m.attributes().size());

		assertEquals("2018-01-01T00:00:16Z", m.get("ts").asString());
		assertEquals("someemail@gmail.com\nother_email@outlook.es", m.get("emails").asString());
		assertEquals("ABC", m.get("cuenta").asString());
		assertEquals("default", m.get("ss").asString());
	}

	@Test
	public void should_read_message_with_empty_attributes() {
		String inl = "[Ingreso]\n" +
				"ts: 2020-05-20T15:36:27.046617Z\n" +
				"id: 202005_123456\n" +
				"fecha: 2020-05-19T00:00:00Z\n" +
				"numeroDocumento: 102345\n" +
				"division: DC\n" +
				"referencia: 000234000010\n" +
				"observaciones: \n" +
				"origenIngreso: Banco\n" +
				"cecodiv: asrd3\n" +
				"importe: 1220\n" +
				"ss: default";

		Message m = new LegacyMessageReader(inl).next();
		assertNotNull(m);

		assertEquals(11, m.attributes().size());

		assertEquals("2020-05-20T15:36:27.046617Z", m.get("ts").asString());
		assertEquals("202005_123456", m.get("id").asString());
		assertEquals("2020-05-19T00:00:00Z", m.get("fecha").asString());
		assertEquals("102345", m.get("numeroDocumento").asString());
		assertEquals("DC", m.get("division").asString());
		assertEquals("000234000010", m.get("referencia").asString());
		assertEquals("", m.get("observaciones").asString());
		assertEquals("Banco", m.get("origenIngreso").asString());
		assertEquals("asrd3", m.get("cecodiv").asString());
		assertEquals("1220", m.get("importe").asString());
		assertEquals("default", m.get("ss").asString());
	}

	@Test
	public void should_read_message_multiline2() {
		String message = "[WARNING]\n" +
				"ts: 2021-07-27T14:28:31.494323Z\n" +
				"source: io.intino.magritte.framework.loaders.ListProcessor:process\n" +
				"message:\n" +
				"\tG@R@34";
		Message next = new LegacyMessageReader(message).next();
		assertNotNull(next);
		assertEquals("G@R@34", next.get("message").asString());
		message = "[WARNING]\n" +
				"ts: 2021-07-27T14:28:31.494323Z\n" +
				"source: io.intino.magritte.framework.loaders.ListProcessor:process\n" +
				"message:G@R@34";
		next = new LegacyMessageReader(message).next();
		assertNotNull(next);
		assertEquals("G@R@34", next.get("message").asString());
	}

	@Test
	public void should_read_message_multiline() {
		String message = "[ERROR]\n" +
				"ts: 2021-07-27T12:50:03.232980Z\n" +
				"source: mx.mediagram.banman.accessor.api.BanmanService$2:onMessage:263\n" +
				"message:\n" +
				"\tjava.util.ConcurrentModificationException\n" +
				"\t\tat java.base/java.util.LinkedHashMap$LinkedHashIterator.nextNode(LinkedHashMap.java:719)\n" +
				"\t\tat java.base/java.util.LinkedHashMap$LinkedEntryIterator.next(LinkedHashMap.java:751)\n" +
				"\t\tat java.base/java.util.LinkedHashMap$LinkedEntryIterator.next(LinkedHashMap.java:749)\n" +
				"\t\tat java.base/java.util.Iterator.forEachRemaining(Iterator.java:133)\n" +
				"\t\tat java.base/java.util.Spliterators$IteratorSpliterator.forEachRemaining(Spliterators.java:1801)\n" +
				"\t\tat java.base/java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:484)\n" +
				"\t\tat java.base/java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:474)\n" +
				"\t\tat java.base/java.util.stream.ReduceOps$ReduceOp.evaluateSequential(ReduceOps.java:913)\n" +
				"\t\tat java.base/java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:234)\n" +
				"\t\tat java.base/java.util.stream.ReferencePipeline.collect(ReferencePipeline.java:578)\n" +
				"\t\tat mx.mediagram.banman.accessor.model.Queue.save(Queue.java:101)\n" +
				"\t\tat mx.mediagram.banman.accessor.model.Queue.save(Queue.java:96)\n" +
				"\t\tat mx.mediagram.banman.accessor.model.Queue.removeMessage(Queue.java:119)\n" +
				"\t\tat mx.mediagram.banman.accessor.model.Queue.removeOutputMessage(Queue.java:47)\n" +
				"\t\tat mx.mediagram.banman.accessor.api.BanmanService$2.onMessage(BanmanService.java:254)\n" +
				"\t\tat org.java_websocket.client.WebSocketClient.onWebsocketMessage(WebSocketClient.java:593)\n" +
				"\t\tat org.java_websocket.drafts.Draft_6455.processFrameText(Draft_6455.java:885)\n" +
				"\t\tat org.java_websocket.drafts.Draft_6455.processFrame(Draft_6455.java:819)\n" +
				"\t\tat org.java_websocket.WebSocketImpl.decodeFrames(WebSocketImpl.java:379)\n" +
				"\t\tat org.java_websocket.WebSocketImpl.decode(WebSocketImpl.java:216)\n" +
				"\t\tat org.java_websocket.client.WebSocketClient.run(WebSocketClient.java:506)\n" +
				"\t\tat java.base/java.lang.Thread.run(Thread.java:834)";
		Message next = new LegacyMessageReader(message).next();
		assertNotNull(next);
	}


	@Test
	public void should_read_message() {
		String message = "[SEVERE]\n" +
				"ts: 2021-04-23T08:20:15.056773Z\n" +
				"source: io.intino.magritte.framework.LayerFactory:create\n" +
				"message: Concept AbstractAcquisition$Device hasn't layer registered. Node Assets#Assets_3962_0_0696810257 won't have it\n";
		Message next = new LegacyMessageReader(message).next();
		assertNotNull(next);
	}

	@Test
	public void should_read_empty_content() {
		LegacyMessageReader messages = new LegacyMessageReader("");
		assertThat(messages.hasNext()).isFalse();
		assertThat(messages.next()).isNull();
	}


	@Test
	public void should_read_messages_in_a_class_with_parent() {
		String inl = "[Teacher]\n" +
				"name: Jose\n" +
				"money: 50.0\n" +
				"birthDate: 1984-11-01T22:34:25Z\n" +
				"university: ULPGC\n" +
				"\n" +
				"[Teacher.Country]\n" +
				"name: Spain\n" +
				"\n" +
				"[Teacher]\n" +
				"name: Juan\n" +
				"money: 40.0\n" +
				"birthDate: 1978-04-02T00:00:00Z\n" +
				"university: ULL\n" +
				"\n" +
				"[Teacher.Country]\n" +
				"name: France\n" +
				"\n" +
				"[Teacher.Country]\n" +
				"name: Germany\n";
		LegacyMessageReader messages = new LegacyMessageReader(inl);

		assertThat(messages.hasNext()).isTrue();
		Message m1 = messages.next();
		assertThat(m1.get("name").as(String.class)).isEqualTo("Jose");
		assertThat(m1.get("money").as(Double.class)).isEqualTo(50.0);
		assertThat(m1.get("birthDate").as(Instant.class)).isEqualTo(instant(1984, 11, 1, 22, 34, 25));
		assertThat(m1.components().size()).isEqualTo(1);
		assertThat(m1.components("country").size()).isEqualTo(1);
		assertThat(m1.components("country").get(0).get("name").as(String.class)).isEqualTo("Spain");

		assertThat(messages.hasNext()).isTrue();
		Message m2 = messages.next();
		assertThat(m2.get("name").as(String.class)).isEqualTo("Juan");
		assertThat(m2.get("money").as(Double.class)).isEqualTo(40.0);
		assertThat(m2.get("birthDate").as(Instant.class)).isEqualTo(instant(1978, 4, 2, 0, 0, 0));
		assertThat(m2.components().size()).isEqualTo(2);
		assertThat(m2.components("country").size()).isEqualTo(2);
		assertThat(m2.components("country").get(0).get("name").as(String.class)).isEqualTo("France");
		assertThat(m2.components("country").get(1).get("name").as(String.class)).isEqualTo("Germany");

		assertThat(messages.hasNext()).isFalse();
		assertThat(messages.next()).isNull();

		assertThat(m1.toString() + "\n" + m2.toString()).isEqualTo(inl);

	}

	@Test
	@Ignore
	public void should_read_messages_with_old_format() {
		String inl = "[Dialog]\n" +
				"instant = 2019-01-14T13:34:09.742Z\n" +
				"opinion = Satisfied\n" +
				"cancelled = false\n" +
				"contactSet = false\n" +
				"contactData = \n" +
				"wantsToBeContacted = false\n" +
				"area = MockChainHotelReception\n" +
				"eventId = \n" +
				"eventLabel = \n" +
				"issueId = \n" +
				"touchCounter = 1\n" +
				"sensorId = 3C15C2CBFF020000\n" +
				"apkVersion = 3.0.21\n" +
				"fingerSizes = 0\n" +
				"hearts = 1\n";
		LegacyMessageReader messages = new LegacyMessageReader(inl);
		Message message = messages.next();
		assertThat(message.contains("issueId")).isFalse();
		assertThat(message.get("wantsToBeContacted").as(Boolean.class)).isFalse();
		assertThat(message.get("hearts").as(Integer.class)).isEqualTo(1);
		assertThat(messages.hasNext()).isFalse();
	}


	@Test
	public void should_read_message_with_multi_lines_and_many_components() {
		String inl = "[Teacher]\n" +
				"name: Jose\u0001Hernandez\n" +
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

		LegacyMessageReader messages = new LegacyMessageReader(inl);
		assertThat(messages.hasNext()).isTrue();
		Message message = messages.next();
		assertThat(message.toString()).isEqualTo(inl);
		assertThat(message.get("name").asString().toCharArray()).contains((char) 1);
		assertThat(message.get("name").as(String[].class)).hasSize(2);
		assertThat(message.components().size()).isEqualTo(3);
		assertThat(message.components("Phone").get(0).components().size()).isEqualTo(1);
		assertThat(messages.hasNext()).isFalse();
		assertThat(messages.next()).isNull();
	}

	@Test
	public void should_read_messages_with_array_attributes() {
		String inl = "[Menu]\n" +
				"meals: " + "Soup\u0001" + "Lobster\u0001" + "Mussels\u0001" + "Cake\n" +
				"prices: " + "5.0\u0001" + "24.5\u0001" + "8.0\u0001" + "7.0\n" +
				"availability: " + "true\u0001" + "false\n";

		LegacyMessageReader messages = new LegacyMessageReader(inl);
		assertThat(messages.hasNext()).isTrue();
		Message message = messages.next();
		assertThat(message.toString()).isEqualTo(inl);
		assertThat(message.get("meals").as(String[].class)).isEqualTo(new String[]{"Soup", "Lobster", "Mussels", "Cake"});
		assertThat(message.get("prices").as(Double[].class)).isEqualTo(new Double[]{5., 24.5, 8., 7.});
		assertThat(message.get("availability").as(Boolean[].class)).isEqualTo(new Boolean[]{true, false});
		assertThat(messages.hasNext()).isFalse();
		assertThat(messages.next()).isNull();
	}


	@Test
	public void should_read_message_with_multiline_attribute1() {
		String inl = "[Contactos.Contacto]\n" +
				"nombre: LIC RAUL ALFONSO CABALLERO CONTRERAS\n" +
				"telefonos:\n" +
				"\t01\n" +
				"\t0177\n" +
				"\t01771\n" +
				"\t01771202\n" +
				"\t017712026837\n" +
				"cargo: CONTRALOR INTERNO\n" +
				"tipo: Comercial\n" +
				"email: raul.caballero@hidalgo.gob\n";
		Message message = new LegacyMessageReader(inl).next();
		String telefonos = message.get("telefonos").asString();
		System.out.println(telefonos);
	}

	@Test
	public void should_read_message_with_multiline_attribut2() {
		String inl = "[ERROR]\n" +
				"ts: 2020-05-05T19:24:32.533342Z\n" +
				"source: systems.intino.eventsourcing.jms.TopicConsumer:close:36\n" +
				"message:\n" +
				"\tjavax.jms.IllegalStateException: The Session is closed\n" +
				"\t\tat org.apache.activemq.ActiveMQSession.checkClosed(ActiveMQSession.java:771)\n" +
				"\t\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)\n" +
				"\t\tat java.base/java.util.concurrent.FutureTask.runAndReset(FutureTask.java:305)\n" +
				"\t\tat java.base/java.lang.Thread.run(Thread.java:834)\n";

		Message message = new LegacyMessageReader(inl).next();
		assertThat(message.get("ts").as(Instant.class)).isEqualTo(Instant.parse("2020-05-05T19:24:32.533342Z"));
		assertThat(message.get("source").as(String.class)).isEqualTo("systems.intino.eventsourcing.jms.TopicConsumer:close:36");
		assertThat(message.get("message").as(String.class)).isEqualTo("javax.jms.IllegalStateException: The Session is closed\n" +
				"\tat org.apache.activemq.ActiveMQSession.checkClosed(ActiveMQSession.java:771)\n" +
				"\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)\n" +
				"\tat java.base/java.util.concurrent.FutureTask.runAndReset(FutureTask.java:305)\n" +
				"\tat java.base/java.lang.Thread.run(Thread.java:834)");
	}

	@Test
	public void should_read_embeded_message() {
		String inl = "[ProcessLog]\n" +
				"ts: 2020-06-18T13:30:58.349496Z\n" +
				"serverId: deploy-alpgc-smartbeach-dev\n" +
				"id: com.monentia.smartbeach:control\n" +
				"value:\n" +
				"\t[INFO]\n" +
				"\tts: 2020-06-18T13:30:58.329749Z\n" +
				"\tsource: systems.intino.eventsourcing.terminal.JmsConnector$1:transportResumed:306\n" +
				"\tmessage: Connection with Data Hub established!\n";
		Message message = new LegacyMessageReader(inl).next();
		assertThat(message).isNotNull();
		assertThat(message.get("value").asString()).isNotNull();
	}

	@Test
	public void should_read_log_message() {
		String inl = "[ERROR]\n" +
				"ts: 2020-06-19T11:00:52.105720Z\n" +
				"source: spark.http.matching.GeneralError:modify\n" +
				"message:\n" +
				"\tCaused by:\n" +
				"\tjava.lang.NullPointerException\n" +
				"\t\tat com.monentia.smartbeach.control.box.actions.GetSensorAction.execute(GetSensorAction.java:19)\n" +
				"\t\tat com.monentia.smartbeach.control.box.rest.resources.GetSensorResource.execute(GetSensorResource.java:25)\n" +
				"\t\tat com.monentia.smartbeach.control.box.ControlServiceService.lambda$setup$4(ControlServiceService.java:14)\n" +
				"\t\tat systems.intino.eventsourcing.http.spark.SparkRouter.execute(SparkRouter.java:97)\n" +
				"\t\tat systems.intino.eventsourcing.http.spark.SparkRouter.lambda$get$1(SparkRouter.java:33)\n" +
				"\t\tat spark.RouteImpl$1.handle(RouteImpl.java:72)\n" +
				"\t\tat spark.http.matching.Routes.execute(Routes.java:61)\n" +
				"\t\tat spark.http.matching.MatcherFilter.doFilter(MatcherFilter.java:130)\n" +
				"\t\tat spark.embeddedserver.jetty.JettyHandler.doHandle(JettyHandler.java:50)\n" +
				"\t\tat org.eclipse.jetty.server.session.SessionHandler.doScope(SessionHandler.java:1568)\n" +
				"\t\tat org.eclipse.jetty.server.handler.ScopedHandler.handle(ScopedHandler.java:141)\n" +
				"\t\tat org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:132)\n" +
				"\t\tat org.eclipse.jetty.server.Server.handle(Server.java:530)\n" +
				"\t\tat org.eclipse.jetty.server.HttpChannel.handle(HttpChannel.java:347)\n" +
				"\t\tat org.eclipse.jetty.server.HttpConnection.onFillable(HttpConnection.java:256)\n" +
				"\t\tat org.eclipse.jetty.io.AbstractConnection$ReadCallback.succeeded(AbstractConnection.java:279)\n" +
				"\t\tat org.eclipse.jetty.io.FillInterest.fillable(FillInterest.java:102)\n" +
				"\t\tat org.eclipse.jetty.io.ChannelEndPoint$2.run(ChannelEndPoint.java:124)\n" +
				"\t\tat org.eclipse.jetty.util.thread.strategy.EatWhatYouKill.doProduce(EatWhatYouKill.java:247)\n" +
				"\t\tat org.eclipse.jetty.util.thread.strategy.EatWhatYouKill.produce(EatWhatYouKill.java:140)\n" +
				"\t\tat org.eclipse.jetty.util.thread.strategy.EatWhatYouKill.run(EatWhatYouKill.java:131)\n" +
				"\t\tat org.eclipse.jetty.util.thread.ReservedThreadExecutor$ReservedThread.run(ReservedThreadExecutor.java:382)\n" +
				"\t\tat org.eclipse.jetty.util.thread.QueuedThreadPool.runJob(QueuedThreadPool.java:708)\n" +
				"\t\tat org.eclipse.jetty.util.thread.QueuedThreadPool$2.run(QueuedThreadPool.java:626)\n" +
				"\t\tat java.base/java.lang.Thread.run(Thread.java:834)";
		Message message = new LegacyMessageReader(inl).next();
		assertThat(message).isNotNull();
		assertThat(message.get("message").asString()).isNotNull();
	}

	private Instant instant(int y, int m, int d, int h, int mn, int s) {
		return LocalDateTime.of(y, m, d, h, mn, s).atZone(ZoneId.of("UTC")).toInstant();
	}

}
