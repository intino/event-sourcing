package benchmarks;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.junit.Ignore;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 2, warmups = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Ignore
public class CompressionBenchmark {

	private static final byte[] INL = loadInl();

	@Benchmark
	public void _1zstd_write() throws IOException {
		write("zstd", ZstdOutputStream::new);
	}

	@Benchmark
	public void _2snappy_write() throws IOException {
		write("snappy", SnappyOutputStream::new);
	}

	@Benchmark
	public void _3lz4_write() throws IOException {
		write("lz4", LZ4BlockOutputStream::new);
	}

	@Benchmark
	public void _4zstd_read(Blackhole blackhole) throws IOException {
		read("zstd", ZstdInputStream::new, blackhole);
	}

	@Benchmark
	public void _5snappy_read(Blackhole blackhole) throws IOException {
		read("snappy", SnappyInputStream::new, blackhole);
	}

	@Benchmark
	public void _6lz4_read(Blackhole blackhole) throws IOException {
		read("lz4", LZ4BlockInputStream::new, blackhole);
	}

	private static void write(String name, Factory<OutputStream> outStreamFactory) throws IOException {
		File file = new File("../temp/compression/test." + name);
		file.getParentFile().mkdirs();
		try(OutputStream out = outStreamFactory.create(new BufferedOutputStream(new FileOutputStream(file)))) {
			ByteArrayInputStream in = new ByteArrayInputStream(INL);
			byte[] buffer = new byte[1024];
			int read;
			while((read = in.read(buffer)) > 0) {
				out.write(buffer, 0, read);
			}
		}
	}

	private static void read(String name, Factory<InputStream> inStreamFactory, Blackhole bk) throws IOException {
		File file = new File("../temp/compression/test." + name);
		try(InputStream in = inStreamFactory.create(new BufferedInputStream(new FileInputStream(file)))) {
			byte[] buffer = new byte[1024];
			int read;
			while((read = in.read(buffer)) > 0) {
				bk.consume(buffer);
				bk.consume(read);
			}
		}
	}

	private static byte[] loadInl() {
		try {
			return Files.readAllBytes(new File("C:\\Users\\naits\\Desktop\\MonentiaDev\\alexandria\\temp\\events.inl").toPath());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	interface Factory<T> {
		T create(T args) throws IOException;
	}
}
