package systems.intino.eventsourcing.sealing;

import systems.intino.eventsourcing.datalake.Datalake;

import java.util.function.Predicate;

public interface SessionSealer {

	default void seal() {
		seal(TankFilter.acceptAll());
	}

	void seal(TankFilter tankFilter);

	interface TankFilter extends Predicate<Datalake.Store.Tank<?>> {

		static TankFilter acceptAll() {
			return tank -> true;
		}

		boolean accepts(Datalake.Store.Tank<?> tank);

		@Override
		default boolean test(Datalake.Store.Tank<?> tank) {return accepts(tank);}
	}

	interface TankNameFilter extends Predicate<String> {

		static TankNameFilter acceptAll() {
			return name -> true;
		}

		boolean accepts(String tankName);

		@Override
		default boolean test(String tankName) {return accepts(tankName);}
	}
}
