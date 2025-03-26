package systems.intino.eventsourcing.event.resource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ZipFileEntryInputStream extends InputStream {
	private final ZipFile zipFile;
	private final InputStream inputStream;

	public ZipFileEntryInputStream(String filename, String entryName) throws IOException {
		zipFile = new ZipFile(filename);
		inputStream = zipFile.getInputStream(zipFile.getEntry(entryName));
	}

	public ZipFileEntryInputStream(ZipFile file, ZipEntry entry) throws IOException {
		zipFile = file;
		inputStream = zipFile.getInputStream(entry);
	}

	@Override
	public int read() throws IOException {
		return inputStream.read();
	}

	@Override
	public int read(byte[] b) throws IOException {
		return inputStream.read(b);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return inputStream.read(b, off, len);
	}

	@Override
	public byte[] readAllBytes() throws IOException {
		return inputStream.readAllBytes();
	}

	@Override
	public byte[] readNBytes(int len) throws IOException {
		return inputStream.readNBytes(len);
	}

	@Override
	public int readNBytes(byte[] b, int off, int len) throws IOException {
		return inputStream.readNBytes(b, off, len);
	}

	@Override
	public long skip(long n) throws IOException {
		return inputStream.skip(n);
	}

	@Override
	public int available() throws IOException {
		return inputStream.available();
	}

	@Override
	public void close() throws IOException {
		inputStream.close();
		zipFile.close();
	}

	@Override
	public void mark(int readlimit) {
		inputStream.mark(readlimit);
	}

	@Override
	public void reset() throws IOException {
		inputStream.reset();
	}

	@Override
	public boolean markSupported() {
		return inputStream.markSupported();
	}

	@Override
	public long transferTo(OutputStream out) throws IOException {
		return inputStream.transferTo(out);
	}
}