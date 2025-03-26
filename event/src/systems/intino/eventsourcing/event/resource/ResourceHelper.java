package systems.intino.eventsourcing.event.resource;

import com.google.gson.reflect.TypeToken;
import io.intino.alexandria.Json;

import java.io.File;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class ResourceHelper {

	public static final String METADATA_TYPE = "$type";
	public static final String METADATA_SS = "$ss";
	public static final String METADATA_TS = "$ts";
	public static final String METADATA_REI = "$rei";
	public static final String METADATA_FILE = "$file";

	public static String serializeMetadata(ResourceEvent event, File file) {
		Map<String, String> metadata = new HashMap<>(event.resource().metadata().properties());
		metadata.put(METADATA_TYPE, event.type());
		metadata.put(METADATA_SS, event.ss());
		metadata.put(METADATA_TS, event.ts().toString());
		metadata.put(METADATA_REI, event.getREI().toString());
		if (file != null) metadata.put(METADATA_FILE, file.getAbsolutePath());
		return Json.toJson(metadata);
	}

	public static Map<String, String> deserializeMetadata(String content) {
		try {
			return Json.fromJson(content, asMap);
		} catch (com.google.gson.JsonSyntaxException e) {
			return null;
		}
	}

	public static final Type asMap = new TypeToken<Map<String, String>>() {}.getType();
}
