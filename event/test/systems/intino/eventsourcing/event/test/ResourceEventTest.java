package systems.intino.eventsourcing.event.test;

import io.intino.alexandria.Resource;
import systems.intino.eventsourcing.event.resource.ResourceEvent;
import systems.intino.eventsourcing.event.resource.ResourceEventReader;
import systems.intino.eventsourcing.event.resource.ResourceEventWriter;
import io.intino.alexandria.logger.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class ResourceEventTest {


	@Test
	public void name() throws IOException {
		ResourceEvent event = new ResourceEvent("Log", "linode", new Resource("./test-res/resource.txt", new FileInputStream("./test-res/resource.txt")));
		String pathname = "./resource.zip";
		File result = new File(pathname);
		result.delete();
		ResourceEventWriter resourceEventWriter = new ResourceEventWriter(new File(pathname));
		resourceEventWriter.write(event);
		resourceEventWriter.close();
		try (ResourceEventReader resourceEventReader = new ResourceEventReader(new File(pathname))) {
			resourceEventReader.forEachRemaining(e -> {
				try {
					Assert.assertEquals("./test-res/resource.txt", e.getREI().resourceName());
					Assert.assertEquals(32, e.resource().readAsString().lines().count());
				} catch (IOException ex) {
					Logger.error(ex);
				}

			});
		} catch (Exception e) {
			Logger.error(e);
		}
		result.delete();
	}
}
