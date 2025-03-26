package io.intino.performance;

import systems.intino.eventsourcing.message.MessageStream;
import io.intino.performance.impl.MessageStreamOld;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.*;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 3, warmups = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
public class MessageStreamBenchmark {
//	Windows 10, i9-10885H 2.40 GHz
//	Benchmark                  Mode  Cnt    Score    Error  Units
//	MessageStreamBenchmark.v1  avgt   15  261,224 ± 32,376  ms/op
//	MessageStreamBenchmark.v2  avgt   15  165,870 ± 14,878  ms/op

	private static final byte[] INL = loadInl().getBytes();

	@Benchmark
	public Blackhole v1(Blackhole bk) {
		Iterator<String> iterator = new MessageStreamOld(new ByteArrayInputStream(INL));
		while(iterator.hasNext()) {
			bk.consume(iterator.next());
		}
		return bk;
	}

	@Benchmark
	public Blackhole v2(Blackhole bk) {
		Iterator<String> iterator = new MessageStream(new ByteArrayInputStream(INL));
		while(iterator.hasNext()) {
			bk.consume(iterator.next());
		}
		return bk;
	}

	private static String loadInl() {
		try {
			return Files.readString(new File("C:\\Users\\naits\\Desktop\\MonentiaDev\\alexandria\\temp\\messages.inl").toPath());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
