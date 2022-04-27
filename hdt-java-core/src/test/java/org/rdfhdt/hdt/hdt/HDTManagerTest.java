package org.rdfhdt.hdt.hdt;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult;
import org.rdfhdt.hdt.options.HDTOptionsKeys;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.triples.impl.utils.HDTTestUtils;
import org.rdfhdt.hdt.util.LargeFakeDataSetStreamSupplier;
import org.rdfhdt.hdt.util.StopWatch;
import org.rdfhdt.hdt.util.io.compress.CompressTest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;

public class HDTManagerTest {
	private static final long SIZE = 1L << 16;
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();
	private HDTSpecification spec;

	@Before
	public void setup() throws IOException {
		spec = new HDTSpecification();
		spec.set("loader.disk.location", tempDir.newFolder().getAbsolutePath());
	}

	private void generateDiskTest() throws IOException, ParserException, NotFoundException {
		LargeFakeDataSetStreamSupplier supplier =
				LargeFakeDataSetStreamSupplier
						.createSupplierWithMaxSize(SIZE * 8, 34)
						.withMaxElementSplit(20)
						.withMaxLiteralSize(50);

		// create DISK HDT
		HDT actual = HDTManager.generateHDTDisk(
				supplier.createTripleStringStream(),
				HDTTestUtils.BASE_URI,
				spec,
				(level, message) -> System.out.println("[" + level + "] " + message)
		);

		supplier.reset();

		// create MEMORY HDT
		HDT expected = HDTManager.generateHDT(
				supplier.createTripleStringStream(),
				HDTTestUtils.BASE_URI,
				spec,
				null
		);

		try {
			assertEquals(expected, actual);
		} finally {
			try {
				expected.close();
			} finally {
				actual.close();
			}
		}
	}

	private void assertEquals(HDT expected, HDT actual) throws NotFoundException {

		// test dictionary
		Dictionary ed = expected.getDictionary();
		Dictionary ad = actual.getDictionary();
		assertEquals("Subjects", ed.getSubjects(), ad.getSubjects());
		assertEquals("Predicates", ed.getPredicates(), ad.getPredicates());
		assertEquals("Objects", ed.getObjects(), ad.getObjects());
		assertEquals("Shared", ed.getShared(), ad.getShared());
		Assert.assertEquals(ed.getType(), ad.getType());

		// test triples
		IteratorTripleString actualIt = actual.search("", "", "");
		IteratorTripleString expectedIt = expected.search("", "", "");

		while (expectedIt.hasNext()) {
			Assert.assertTrue(actualIt.hasNext());

			TripleString expectedTriple = expectedIt.next();
			TripleString actualTriple = actualIt.next();
			Assert.assertEquals(expectedIt.getLastTriplePosition(), actualIt.getLastTriplePosition());
			Assert.assertEquals(expectedTriple, actualTriple);
		}
		Assert.assertFalse(actualIt.hasNext());

		// test header
		Assert.assertEquals(actual.getHeader().getBaseURI(), expected.getHeader().getBaseURI());
		Assert.assertEquals(actual.getHeader().getNumberOfElements(), expected.getHeader().getNumberOfElements());
	}

	private void assertEquals(String section, DictionarySection excepted, DictionarySection actual) {
		Iterator<? extends CharSequence> itEx = excepted.getSortedEntries();
		Iterator<? extends CharSequence> itAc = actual.getSortedEntries();

		while (itEx.hasNext()) {
			Assert.assertTrue(itAc.hasNext());
			CharSequence expectedTriple = itEx.next();
			CharSequence actualTriple = itAc.next();
			CompressTest.assertCharSequenceEquals(section + " section strings", expectedTriple, actualTriple);
		}
		Assert.assertFalse(itAc.hasNext());
		Assert.assertEquals(excepted.getNumberOfElements(), actual.getNumberOfElements());
	}

	@Test
	public void generateSaveLoadMapTest() throws IOException, ParserException, NotFoundException {
		LargeFakeDataSetStreamSupplier supplier =
				LargeFakeDataSetStreamSupplier
						.createSupplierWithMaxSize(SIZE * 8, 34)
						.withMaxElementSplit(20)
						.withMaxLiteralSize(50);

		// create MEMORY HDT
		HDT expected = HDTManager.generateHDT(
				supplier.createTripleStringStream(),
				HDTTestUtils.BASE_URI,
				spec,
				null
		);
		String tmp = tempDir.newFile().getAbsolutePath();
		expected.saveToHDT(tmp, null);

		HDT mapExcepted;

		try {
			mapExcepted = HDTManager.mapHDT(tmp, null);
			try {
				assertEquals(expected, mapExcepted);
			} finally {
				mapExcepted.close();
			}

			mapExcepted = HDTManager.loadHDT(tmp, null);
			try {
				assertEquals(expected, mapExcepted);
			} finally {
				mapExcepted.close();
			}
		} finally {
			expected.close();
		}

	}

	@Test
	public void generateDiskCompleteTest() throws IOException, ParserException, NotFoundException {
		spec.set("loader.disk.compressMode", CompressionResult.COMPRESSION_MODE_COMPLETE);
		spec.setInt("loader.disk.chunkSize", SIZE);
		generateDiskTest();
	}

	@Test
	public void generateDiskPartialTest() throws IOException, ParserException, NotFoundException {
		spec.set("loader.disk.compressMode", CompressionResult.COMPRESSION_MODE_PARTIAL);
		spec.setInt("loader.disk.chunkSize", SIZE);
		generateDiskTest();
	}

	@Test
	public void generateDiskCompleteMapTest() throws IOException, ParserException, NotFoundException {
		spec.set("loader.disk.compressMode", CompressionResult.COMPRESSION_MODE_COMPLETE);
		spec.setInt("loader.disk.chunkSize", SIZE);
		File mapHDT = tempDir.newFile("mapHDTTest.hdt");
		spec.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, mapHDT.getAbsolutePath());
		generateDiskTest();
		Files.deleteIfExists(mapHDT.toPath());
	}

	@Test
	public void generateDiskPartialMapTest() throws IOException, ParserException, NotFoundException {
		spec.set("loader.disk.compressMode", CompressionResult.COMPRESSION_MODE_PARTIAL);
		spec.setInt("loader.disk.chunkSize", SIZE);
		File mapHDT = tempDir.newFile("mapHDTTest.hdt");
		spec.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, mapHDT.getAbsolutePath());
		generateDiskTest();
		Files.deleteIfExists(mapHDT.toPath());
	}

	@Test
	@Ignore("big")
	public void customLoadTest() throws IOException, ParserException, NotFoundException {
		spec.set("loader.disk.compressMode", CompressionResult.COMPRESSION_MODE_COMPLETE);
		File mapHDT = tempDir.newFile("mapHDTTest.hdt");
		spec.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, mapHDT.getAbsolutePath());

		StopWatch watch = new StopWatch();
		HDT actual = HDTManager.generateHDTDisk(
				// create a big nt file and put it here:
				"/Users/ate/workspace/qacompany/dbpedia/dataset.nt.gz",
				HDTTestUtils.BASE_URI,
				spec,
				(level, message) -> System.out.println("[" + level + "] " + message)
		);
		System.out.println(watch.stopAndShow());
		watch.reset();
		HDT expected = HDTManager.generateHDT(
				"/Users/ate/workspace/qacompany/dbpedia/dataset.nt.gz",
				HDTTestUtils.BASE_URI,
				RDFNotation.NTRIPLES,
				spec,
				(level, message) -> {
				}
		);
		System.out.println(watch.stopAndShow());

		assertEquals(expected, actual);
	}
}
