package systems.intino.eventsoucing.sealing.sorters;

import systems.intino.eventsoucing.sealing.EventSorter;

import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class ResourceEventSorter extends EventSorter {

	public ResourceEventSorter(File file, File tempFolder) throws IOException {
		super(file, File.createTempFile("event", ".zip", tempFolder));
	}

	@Override
	public void sort(File destination) throws IOException {
		ZipEntryList entryList = listZipEntriesOf(file);
		if(entryList.isEmpty() || entryList.isSorted()) return;
		sortZipEntries(entryList.entries, destination);
	}

	private void sortZipEntries(String[] entries, File destination) throws IOException {
		try {
			Arrays.sort(entries);
			writeTheSortedEntriesIntoTheTempZipFile(entries);
			Files.move(temp.toPath(), destination.toPath(), REPLACE_EXISTING);
		} finally {
			temp.delete();
		}
	}

	private void writeTheSortedEntriesIntoTheTempZipFile(String[] entries) throws IOException {
		try(ZipOutputStream zipOut = openZipForWriting(temp); ZipFile zipIn = new ZipFile(file)) {
			for(String entryName : entries) {
				ZipEntry entry = zipIn.getEntry(entryName);
				zipOut.putNextEntry(entry);
				try(InputStream entryData = zipIn.getInputStream(entry)) {
					entryData.transferTo(zipOut);
				}
			}
		}
	}

	private ZipEntryList listZipEntriesOf(File file) throws IOException {
		try(ZipFile zip = new ZipFile(file)) {
			String[] prevName = new String[1];
			boolean[] sorted = new boolean[]{true};
			String[] entries = zip.stream()
					.map(ZipEntry::getName)
					.peek(entry -> saveSortingInfo(entry, prevName, sorted))
					.toArray(String[]::new);
			return new ZipEntryList(entries, sorted[0]);
		}
	}

	private void saveSortingInfo(String entry, String[] prevName, boolean[] sorted) {
		if(!sorted[0]) return;
		if(prevName[0] != null) sorted[0] = prevName[0].compareTo(entry) <= 0;
		prevName[0] = entry;
	}

	private static ZipOutputStream openZipForWriting(File temp) throws IOException {
		return new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(temp)));
	}

	private static class ZipEntryList {

		private final String[] entries;
		private final boolean sorted;

		public ZipEntryList(String[] entries, boolean sorted) {
			this.entries = entries;
			this.sorted = sorted;
		}

		public boolean isEmpty() {
			return entries.length == 0;
		}

		public boolean isSorted() {
			return sorted;
		}
	}
}
