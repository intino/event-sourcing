package systems.intino.eventsourcing.datalake.tests;

import systems.intino.eventsourcing.datalake.Datalake;
import systems.intino.eventsourcing.datalake.file.FileDatalake;
import systems.intino.eventsourcing.event.message.MessageEvent;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 2, warmups = 0)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
@SuppressWarnings("all")
public class ReflowBenchmark {

	private static final Datalake datalake = new FileDatalake(new File("C:\\Users\\naits\\Desktop\\MonentiaDev\\alexandria\\temp\\cm\\new_datalake"));
	private static final Set<String> tankNames = Set.of(
			"contratacion.Contrato",
			"comercial.cuentamaestra.Adeudo",
			"ComandoEjecutado",
			"comercial.cuentamaestra.GestionComercialSolicitada",
			"facturacion.RemesaServicioCargada",
			"federation.Identities"
	);
	private static final Datalake.Store.Tank[] tanks;
	static {
		tanks = datalake.messageStore().tanks()
				.filter(t -> tankNames.contains(t.name()))
				.toArray(Datalake.Store.Tank[]::new);
	}

	@Benchmark
	public Blackhole reflow(Blackhole blackhole) {
		for(Datalake.Store.Tank<MessageEvent> tank : tanks) {
			tank.sources().flatMap(Datalake.Store.Source::tubs).forEach(tub -> tub.events().forEach(blackhole::consume));
		}
		return blackhole;
	}
}
