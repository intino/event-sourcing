package systems.intino.eventsourcing.zim;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Zim {

	public static final String ZIM_EXTENSION = ".zim";

	public static InputStream decompressing(InputStream in) throws IOException {
		return in instanceof ZstdInputStream ? in : new ZstdInputStream(in);
	}

	public static OutputStream compressing(OutputStream out) throws IOException {
		return out instanceof ZstdOutputStream ? out : new ZstdOutputStream(out);
	}
}
