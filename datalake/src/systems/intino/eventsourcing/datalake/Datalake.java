package systems.intino.eventsourcing.datalake;

import io.intino.alexandria.Scale;
import io.intino.alexandria.Timetag;
import systems.intino.eventsourcing.event.Event;
import systems.intino.eventsourcing.event.message.MessageEvent;
import systems.intino.eventsourcing.event.resource.ResourceEvent;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static systems.intino.eventsourcing.event.EventStream.merge;
import static systems.intino.eventsourcing.event.EventStream.sequence;

public interface Datalake {
	String MessageStoreFolder = "messages";
	String ResourceStoreFolder = "resources";

	Store<MessageEvent> messageStore();

	ResourceStore resourceStore();

	interface Store<T extends Event> {
		Stream<Tank<T>> tanks();

		default boolean containsTank(String tank) {
			return tanks().anyMatch(t -> t.name().equals(tank));
		}

		Tank<T> tank(String name);

		default Stream<T> content() {
			return tanks().flatMap(Tank::content);
		}

		default Scale scale() {
			return tanks().parallel().map(Tank::scale).filter(Objects::nonNull).findAny().orElse(null);
		}

		interface Tank<T extends Event> {
			String name();

			default Scale scale() {
				List<Source<T>> sources = sources().toList();
				return sources.isEmpty() ? null : sources.getFirst().scale();
			}

			Source<T> source(String name);

			Stream<Source<T>> sources();

			default Stream<T> content() {
				return merge(sources().map(s -> sequence(s.tubs().map(Tub::eventSupplier).collect(Collectors.toList()))));
			}

			default Stream<T> content(BiPredicate<Source<T>, Timetag> filter) {
				return merge(sources().map(s -> sequence(s.tubs().filter(t -> filter.test(s, t.timetag())).map(Tub::eventSupplier).collect(Collectors.toList()))));
			}
		}

		interface Source<T extends Event> {
			String name();

			default Tub<T> tub(String timetag) {return tub(Timetag.of(timetag));}
			Tub<T> tub(Timetag timetag);

			Stream<Tub<T>> tubs();

			Tub<T> first();

			Tub<T> last();

			Tub<T> on(Timetag tag);

			default Scale scale() {
				return first().timetag().scale();
			}

			default Stream<Tub<T>> tubs(Timetag from, Timetag to) {
				return StreamSupport.stream(from.iterateTo(to).spliterator(), false).map(this::on);
			}
		}

		interface Tub<T extends Event> {
			Timetag timetag();

			Stream<T> events();

			default Supplier<Stream<T>> eventSupplier() {
				return this::events;
			}

			default Scale scale() {
				return timetag().scale();
			}

			default Stream<T> events(Predicate<T> filter) {
				return events().filter(filter);
			}

			default Supplier<Stream<T>> eventSupplier(Predicate<T> filter) {
				return () -> events(filter);
			}
		}
	}

	interface ResourceStore extends Store<ResourceEvent> {
		default Optional<ResourceEvent> find(String rei) {return find(ResourceEvent.REI.of(rei));}
		Optional<ResourceEvent> find(ResourceEvent.REI rei);
	}
}