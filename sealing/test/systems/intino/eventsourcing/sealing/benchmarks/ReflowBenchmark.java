package systems.intino.eventsourcing.sealing.benchmarks;

import systems.intino.eventsourcing.zim.ZimStream;
import systems.intino.eventsourcing.zim.ZimWriter;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import systems.intino.eventsourcing.message.Message;
import systems.intino.eventsourcing.message.MessageReader;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 2, warmups = 0)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
public class ReflowBenchmark {

	private static final File InlDir = new File("C:\\Users\\naits\\Desktop\\MonentiaDev\\alexandria\\temp\\datalake\\events\\inl");
	private static final File EventsDir = new File("C:\\Users\\naits\\Desktop\\MonentiaDev\\alexandria\\temp\\datalake\\events\\events");

	private static final File[] ZimFiles = FileUtils.listFiles(EventsDir, new String[] {".zim", "zim"}, true).toArray(File[]::new);

	@Benchmark
	public Blackhole reflow(Blackhole blackhole) throws IOException {
		for(File zim : ZimFiles) {
			try(Stream<Message> stream = ZimStream.of(zim)) {
				stream.forEach(blackhole::consume);
			}
		}
		return blackhole;
	}

//	@Ignore
	@Test
	//9896487 events in total
	public void test() throws Exception {
		long total = 0;
		for(File zim : ZimFiles) {
			System.out.print(zim);
			int size = 0;
			try(Stream<Message> stream = ZimStream.of(zim)) {
				List<Message> inlMessages = fromInl(zim);
				List<Message> zimMessages = stream.collect(Collectors.toList());
				size = inlMessages.size();
				assertTrue(inlMessages.size() > 1);
				assertEquals(inlMessages.size(), zimMessages.size());
				for(int i = 0;i < inlMessages.size();i++) {
					assertEquals(inlMessages.get(i), zimMessages.get(i));
				}
			}
			System.out.println("... (" + size + " events)");
			total += size;
		}
		System.out.println("DONE -> " + total + " events in total");
	}

	private List<Message> fromInl(File zim) throws Exception {
		List<Message> messages = new ArrayList<>();
		try(var reader = new MessageReader(new BufferedInputStream(new FileInputStream(new File(InlDir, zim.getParentFile().getName() + "/" + zim.getName().replace("zim", "inl")))))) {
			while(reader.hasNext()) messages.add(reader.next());
		}
		return messages;
	}

	public static void main1(String[] args) throws Exception {
		convertInlToZim(InlDir);
		System.out.println("done");
	}

	private static void convertInlToZim(File dir) throws Exception {
		for(File file : dir.listFiles()) {
			if(file.isDirectory()) {
				convertInlToZim(file);
			} else {
				if(!file.getName().endsWith("inl")) {
					file.delete();
					continue;
				}
				File zim = new File(EventsDir, dir.getName() + "/" + file.getName().replace("inl", "zim"));
				zim.getParentFile().mkdirs();
				System.out.println(zim);
				try(ZimWriter writer = new ZimWriter(zim)) {
					try(MessageReader reader = new MessageReader(new BufferedInputStream(new FileInputStream(file)))) {
						while(reader.hasNext()) {
							writer.write(reader.next());
						}
					}
				}
			}
		}
	}
}
