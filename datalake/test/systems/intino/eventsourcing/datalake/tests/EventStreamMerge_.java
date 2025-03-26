package systems.intino.eventsourcing.datalake.tests;

import systems.intino.eventsourcing.datalake.Datalake;
import systems.intino.eventsourcing.datalake.file.FileDatalake;
import systems.intino.eventsourcing.event.EventStream;
import systems.intino.eventsourcing.event.message.MessageEvent;

import java.io.File;
import java.util.*;

public class EventStreamMerge_ {

	public static void main(String[] args) {
		Iterator<MessageEvent> iterator = EventStream.merge(new FileDatalake(new File("C:/Users/naits/Desktop/MonentiaDev/gestioncomercial/temp/cm/new_datalake")).messageStore().tanks()
//				.filter(tank -> tank.name().startsWith("facturacion.") || tank.name().equals("contratacion.Contrato") || tank.name().equals("federation.Identities"))
				.map(Datalake.Store.Tank::content)).iterator();

		MessageEvent ref = null;
		MessageEvent lastEvent = null;

//		Map<String, MessageEvent[]> map = new HashMap<>();
//		Map<String, Integer> indices = new HashMap<>();

		long start = System.currentTimeMillis();

		while(iterator.hasNext()) {
			MessageEvent event = iterator.next();
			ref = event;
//			MessageEvent[] array = map.computeIfAbsent(event.type(), k -> new MessageEvent[1000]);
//			int index = indices.compute(event.type(), (k, v) -> v == null ? 0 : v + 1);
//			array[index % array.length] = event;
			if(lastEvent != null && lastEvent.ts().isAfter(event.ts())) {
				System.out.println(lastEvent);
				System.out.println();
				System.out.println(event);
				return;
			}
			lastEvent = event;
		}

		long time = System.currentTimeMillis() - start;

		System.out.println(ref);

//		map.entrySet().stream()
//				.filter(e -> e.getKey().contains("MedicionGeneracion"))
//				.forEach(e -> Arrays.stream(e.getValue()).filter(Objects::nonNull).map(Objects::toString).forEach(System.out::println));

		System.out.println("Time: " + time/1000.0f + " seconds");
	}
}
