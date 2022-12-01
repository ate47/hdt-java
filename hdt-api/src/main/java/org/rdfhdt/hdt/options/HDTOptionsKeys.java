package org.rdfhdt.hdt.options;

import org.rdfhdt.hdt.hdt.HDTVocabulary;
import org.rdfhdt.hdt.rdf.RDFFluxStop;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * keys usable with {@link org.rdfhdt.hdt.options.HDTOptions#set(String, String)}
 *
 * @author Antoine Willerval
 */
public class HDTOptionsKeys {
	/**
	 * Key for the compression mode for the {@link org.rdfhdt.hdt.hdt.HDTManager} generateHDTDisk methods.
	 * Value can be {@link #LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE} or
	 * {@link #LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE}
	 */
	@Key(type = Key.Type.ENUM, desc = "Compression mode")
	public static final String LOADER_DISK_COMPRESSION_MODE_KEY = "loader.disk.compressMode";
	/**
	 * Value for {@link #LOADER_DISK_COMPRESSION_MODE_KEY}, sort all the file before going to the next step, slower
	 * but decrease the RAM usage. default config.
	 */
	@Value(key = LOADER_DISK_COMPRESSION_MODE_KEY, desc = "sort all the file before going to the next step, slower but decrease the RAM usage. default config")
	public static final String LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE = "compressionComplete";
	/**
	 * Value for {@link #LOADER_DISK_COMPRESSION_MODE_KEY}, sort while reading all the file before going to the next
	 * step, faster but increase the RAM usage.
	 */
	@Value(key = LOADER_DISK_COMPRESSION_MODE_KEY, desc = "sort while reading all the file before going to the next step, faster but increase the RAM usage.")
	public static final String LOADER_DISK_COMPRESSION_MODE_VALUE_PARTIAL = "compressionPartial";

	/**
	 * Key for the {@link org.rdfhdt.hdt.hdt.HDTManager} generateHDTDisk methods,
	 * say the number of workers to merge the data. default to the number of processor. long value.
	 */
	@Key(type = Key.Type.NUMBER, desc = "Number of core used to compress the HDT")
	public static final String LOADER_DISK_COMPRESSION_WORKER_KEY = "loader.disk.compressWorker";
	/**
	 * Key for the maximum size of a chunk on disk for the {@link org.rdfhdt.hdt.hdt.HDTManager} generateHDTDisk
	 * methods, the chunk should be in RAM before writing it on disk and should be sorted. long value.
	 */
	@Key(type = Key.Type.NUMBER, desc = "Maximum size of a chunk")
	public static final String LOADER_DISK_CHUNK_SIZE_KEY = "loader.disk.chunkSize";
	/**
	 * Key for the location of the working directory {@link org.rdfhdt.hdt.hdt.HDTManager} generateHDTDisk methods,
	 * this directory will be deleted after the HDT generation. by default, the value is random, it is recommended to
	 * set this option to delete the directory in case of an interruption of the process. file value.
	 */
	@Key(type = Key.Type.PATH, desc = "Location of the disk generation directory")
	public static final String LOADER_DISK_LOCATION_KEY = "loader.disk.location";
	/**
	 * Key for the location of the future HDT for the {@link org.rdfhdt.hdt.hdt.HDTManager} generateHDTDisk methods,
	 * this option will create a hdt file after the HDT generation, the returned HDT will be a mapped HDT of the HDT
	 * file. slower, increase the disk usage, but drastically reduce the RAM usage. file value.
	 */
	@Key(type = Key.Type.PATH, desc = "Location of the future HDT")
	public static final String LOADER_DISK_FUTURE_HDT_LOCATION_KEY = "loader.disk.futureHDTLocation";
	/**
	 * Key for the maximum number of file opened at the same time, should be greater than {@link #LOADER_DISK_KWAY_KEY},
	 * 1024 by default
	 */
	@Key(type = Key.Type.NUMBER, desc = "Maximum number of file HDTDisk can open at the same time")
	public static final String LOADER_DISK_MAX_FILE_OPEN_KEY = "loader.disk.maxFileOpen";
	/**
	 * Key for the number of chunk layers opened at the same time, by default
	 * <p>min(log2(maxFileOpen), chunkSize / (fileBufferSize * compressWorker))</p>
	 */
	@Key(type = Key.Type.NUMBER, desc = "log of the number of way the system can merge in genDisk")
	public static final String LOADER_DISK_KWAY_KEY = "loader.disk.kway";
	/**
	 * Key for the size of the buffers when opening a file
	 */
	@Key(type = Key.Type.NUMBER, desc = "Size of the file buffers")
	public static final String LOADER_DISK_BUFFER_SIZE_KEY = "loader.disk.fileBufferSize";
	/**
	 * Key for {@link org.rdfhdt.hdt.hdt.HDTManager#generateHDTDisk(java.util.Iterator, String, HDTOptions, org.rdfhdt.hdt.listener.ProgressListener)},
	 * specify that the method doesn't have to copy the triple strings between 2 calls to the iterator, default false
	 */
	@Key(type = Key.Type.BOOLEAN, desc = "specify that the method doesn't have to copy the triple strings between 2 calls to the iterator")
	public static final String LOADER_DISK_NO_COPY_ITERATOR_KEY = "loader.disk.noCopyIterator";

	/**
	 * Key for the loading mode of a RDF file for the
	 * {@link org.rdfhdt.hdt.hdt.HDTManager#generateHDT(String, String, org.rdfhdt.hdt.enums.RDFNotation, HDTOptions, org.rdfhdt.hdt.listener.ProgressListener)}
	 * method, this key isn't working with the other methods.
	 * Value can be {@link #LOADER_TYPE_VALUE_ONE_PASS}, {@link #LOADER_TYPE_VALUE_TWO_PASS}, {@link #LOADER_TYPE_VALUE_CAT} or {@link #LOADER_TYPE_VALUE_DISK}.
	 */
	@Key(type = Key.Type.ENUM, desc = "HDT generation loader type")
	public static final String LOADER_TYPE_KEY = "loader.type";
	/**
	 * Value for {@link #LOADER_TYPE_KEY}, read using disk generation, reduce the RAM usage and increase disk usage
	 */
	@Value(key = LOADER_TYPE_KEY, desc = "Using genDisk")
	public static final String LOADER_TYPE_VALUE_DISK = "disk";
	/**
	 * Value for {@link #LOADER_TYPE_KEY}, read using HDTCat generation, merge using HDTCat HDT, reduce the RAM usage
	 */
	@Value(key = LOADER_TYPE_KEY, desc = "Using HDTCat")
	public static final String LOADER_TYPE_VALUE_CAT = "cat";
	/**
	 * Value for {@link #LOADER_TYPE_KEY}, read twice the RDF file, reduce the RAM usage
	 */
	@Value(key = LOADER_TYPE_KEY, desc = "Using two pass algorithm")
	public static final String LOADER_TYPE_VALUE_TWO_PASS = "two-pass";
	/**
	 * Value for {@link #LOADER_TYPE_KEY}, read only once the RDF file, default value
	 */
	@Value(key = LOADER_TYPE_KEY, desc = "Using one pass algorithm")
	public static final String LOADER_TYPE_VALUE_ONE_PASS = "one-pass";

	/**
	 * Key for the location of the working directory {@link org.rdfhdt.hdt.hdt.HDTManager} catTree methods,
	 * this directory will be deleted after the HDT generation. by default, the value is random, it is recommended to
	 * set this option to delete the directory in case of an interruption of the process. file value.
	 */
	@Key(type = Key.Type.PATH, desc = "Path of the CatTree generation")
	public static final String LOADER_CATTREE_LOCATION_KEY = "loader.cattree.location";
	/**
	 * Same as {@link #LOADER_TYPE_KEY} for loader in the CATTREE method
	 */
	@Key(desc = "Loader of the hdt generation")
	public static final String LOADER_CATTREE_LOADERTYPE_KEY = "loader.cattree.loadertype";
	/**
	 * Key for the location of the future HDT for the {@link org.rdfhdt.hdt.hdt.HDTManager} catTree methods,
	 * this option will create a hdt file after the HDT generation, the returned HDT will be a mapped HDT of the HDT
	 * file. slower, increase the disk usage, but drastically reduce the RAM usage. file value.
	 */
	@Key(type = Key.Type.PATH, desc = "Location of the future HDT")
	public static final String LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY = "loader.cattree.futureHDTLocation";
	/**
	 * Key for the fault factor for the {@link org.rdfhdt.hdt.hdt.HDTManager} catTree default value of the
	 * split size of the RDFFluxStop in the generateHDT method.
	 */
	@Key(type = Key.Type.DOUBLE, desc = "Memory fault factor for HDTCat tree method split")
	public static final String LOADER_CATTREE_MEMORY_FAULT_FACTOR = "loader.cattree.memoryFaultFactor";
	/**
	 * Key for the k-merge HDTCat for the {@link org.rdfhdt.hdt.hdt.HDTManager} catTree default to 2 using default
	 * implementation of HDTCat, not K-HDTCat
	 */
	@Key(type = Key.Type.NUMBER, desc = "Number of HDT to merge at the same time with K-HDTCat, by default it use the default HDTCat implementation")
	public static final String LOADER_CATTREE_KCAT = "loader.cattree.kcat";

	/**
	 * Key for the hdt supplier type, default to memory
	 */
	@Key(type = Key.Type.ENUM, desc = "HDTCat supplier type")
	public static final String HDT_SUPPLIER_KEY = "supplier.type";
	/**
	 * Value for {@link #HDT_SUPPLIER_KEY}, use HDTGenDisk to create the HDT
	 */
	@Value(key = HDT_SUPPLIER_KEY, desc = "using genDisk")
	public static final String LOADER_CATTREE_HDT_SUPPLIER_VALUE_DISK = "disk";
	/**
	 * Value for {@link #HDT_SUPPLIER_KEY}, use the default memory implementation to create the HDT
	 */
	@Value(key = HDT_SUPPLIER_KEY, desc = "using gen in memory")
	public static final String LOADER_CATTREE_HDT_SUPPLIER_VALUE_MEMORY = "memory";
	/**
	 * Key for the rdf flux stop type, default to the maximum memory allocated
	 */
	@Key(desc = "API use")
	public static final String RDF_FLUX_STOP_KEY = "rdffluxstop.type";
	/**
	 * Value type for the {@link #RDF_FLUX_STOP_KEY}, using {@link RDFFluxStop#asConfig()} would be easier
	 */
	public static final String RDF_FLUX_STOP_VALUE_SIZE = "size";
	/**
	 * Value type for the {@link #RDF_FLUX_STOP_KEY}, using {@link RDFFluxStop#asConfig()} would be easier
	 */
	public static final String RDF_FLUX_STOP_VALUE_COUNT = "count";
	/**
	 * Value type for the {@link #RDF_FLUX_STOP_KEY}, using {@link RDFFluxStop#asConfig()} would be easier
	 */
	public static final String RDF_FLUX_STOP_VALUE_NO_LIMIT = "no_limit";
	/**
	 * Value type for the {@link #RDF_FLUX_STOP_KEY}, using {@link RDFFluxStop#asConfig()} would be easier
	 */
	public static final char RDF_FLUX_STOP_VALUE_OP_AND = '&';
	/**
	 * Value type for the {@link #RDF_FLUX_STOP_KEY}, using {@link RDFFluxStop#asConfig()} would be easier
	 */
	public static final char RDF_FLUX_STOP_VALUE_OP_OR = '|';
	/**
	 * Value type for the {@link #RDF_FLUX_STOP_KEY}, using {@link RDFFluxStop#asConfig()} would be easier
	 */
	public static final char RDF_FLUX_STOP_VALUE_OP_NOT = '!';

	/**
	 * Key for enabling the profiler (if implemented), default to false. Boolean value
	 */
	@Key(type = Key.Type.BOOLEAN, desc = "Use the profiler to get the time of each section")
	public static final String PROFILER_KEY = "profiler";
	/**
	 * Key for the profiler output (if implemented). File value
	 */
	@Key(type = Key.Type.PATH, desc = "Profiler output file")
	public static final String PROFILER_OUTPUT_KEY = "profiler.output";
	/**
	 * Key for enabling the canonical NTriple file simple parser, default to false. Boolean value
	 */
	@Key(type = Key.Type.BOOLEAN, desc = "Use the canonical NT file parser, removing checks")
	public static final String NT_SIMPLE_PARSER_KEY = "parser.ntSimpleParser";
	/**
	 * Key for setting the triple order. see {@link org.rdfhdt.hdt.enums.TripleComponentOrder}'s names to have the values
	 * default to {@link org.rdfhdt.hdt.enums.TripleComponentOrder#SPO}
	 */
	@Key(type = Key.Type.STRING, desc = "HDT generation triple order")
	public static final String TRIPLE_ORDER_KEY = "triplesOrder";

	/**
	 * Option to set how the HDTs are loaded in HDTCat/HDTDiff, default {@link #LOAD_HDT_TYPE_VALUE_MAP}
	 */
	@Key(type = Key.Type.ENUM, desc = "loading type for HDTCat / HDTDiff")
	public static final String LOAD_HDT_TYPE_KEY = "loader.hdt.type";
	/**
	 * load the HDT file into memory
	 */
	@Value(key = LOAD_HDT_TYPE_KEY, desc = "load the HDTs in memory")
	public static final String LOAD_HDT_TYPE_VALUE_LOAD = "load";
	/**
	 * map the HDT file, default value
	 */
	@Value(key = LOAD_HDT_TYPE_KEY, desc = "map the HDTs")
	public static final String LOAD_HDT_TYPE_VALUE_MAP = "map";

	/**
	 * Implementation of the temporary dictionary
	 */
	@Key(type = Key.Type.ENUM, desc = "Internal temporary dictionary")
	public static final String TEMP_DICTIONARY_IMPL_KEY = "tempDictionary.impl";
	/**
	 * use Hash map to create the HDT
	 */
	@Value(key = TEMP_DICTIONARY_IMPL_KEY, desc = "hash dictionary")
	public static final String TEMP_DICTIONARY_IMPL_VALUE_HASH = "hash";
	/**
	 * use Hash map to create the HDT and store the multisection dictionary, mandatory to create MSC
	 */
	@Value(key = TEMP_DICTIONARY_IMPL_KEY, desc = "hash dictionary with literal count")
	public static final String TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH = "multHash";
	/**
	 * use Hash map with Prefix AND Suffix front-coded (PSFC), mandatory to create PSFC dictionary
	 */
	@Value(key = TEMP_DICTIONARY_IMPL_KEY, desc = "Prefix AND Suffix front-coded (PSFC) hash dictionary")
	public static final String TEMP_DICTIONARY_IMPL_VALUE_HASH_PSFC = "hashPsfc";

	/**
	 * Implementation of the dictionary
	 */
	@Key(type = Key.Type.ENUM, desc = "HDT dictionary type")
	public static final String DICTIONARY_TYPE_KEY = "dictionary.type";
	/**
	 * 4 Section dictionary
	 */
	@Value(key = DICTIONARY_TYPE_KEY, desc = "Four sectiob dictionary")
	public static final String DICTIONARY_TYPE_VALUE_FOUR_SECTION = HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION;
	/**
	 * Prefix AND Suffix front-coded (PSFC) 4 Section dictionary
	 */
	@Value(key = DICTIONARY_TYPE_KEY, desc = "Prefix AND Suffix front-coded (PSFC) four section dictionary")
	public static final String DICTIONARY_TYPE_VALUE_FOUR_PSFC_SECTION = HDTVocabulary.DICTIONARY_TYPE_FOUR_PSFC_SECTION;
	/**
	 * big 4 Section dictionary
	 */
	@Value(key = DICTIONARY_TYPE_KEY, desc = "Four section dictionary big")
	public static final String DICTIONARY_TYPE_VALUE_FOUR_SECTION_BIG = "dictionaryFourBig";
	/**
	 * multi section dictionary
	 */
	@Value(key = DICTIONARY_TYPE_KEY, desc = "Multi section dictionary")
	public static final String DICTIONARY_TYPE_VALUE_MULTI_OBJECTS = "dictionaryMultiObj";

	/**
	 * Location of the HDTCat temp files
	 */
	@Key(type = Key.Type.PATH, desc = "Location of the HDTCat temp files")
	public static final String HDTCAT_LOCATION = "hdtcat.location";
	/**
	 * Location of the HDTCat hdt after the loading
	 */
	@Key(type = Key.Type.PATH, desc = "Location of the HDTCat hdt after the loading")
	public static final String HDTCAT_FUTURE_LOCATION = "hdtcat.location.future";
	/**
	 * Delete the HDTCat temp files directory after HDTCat
	 */
	@Key(type = Key.Type.BOOLEAN, desc = "Delete the HDTCat temp files directory after HDTCat, default to true")
	public static final String HDTCAT_DELETE_LOCATION = "hdtcat.deleteLocation";

	// use tree-map to have a better order
	private static final Map<String, Option> OPTION_MAP = new TreeMap<>();

	static {
		try {
			for (Field f : HDTOptionsKeys.class.getDeclaredFields()) {
				Key key = f.getAnnotation(Key.class);
				if (key != null) {
					String keyValue = (String) f.get(null);

					OPTION_MAP.put(keyValue, new Option(keyValue, key));
				} else {
					Value value = f.getAnnotation(Value.class);
					if (value != null) {
						String valueValue = (String) f.get(null);
						Option opt = OPTION_MAP.get(value.key());
						if (opt != null) {
							opt.values.add(new OptionValue(valueValue, value));
						}
					}
				}
			}
		} catch (Exception e) {
			throw new Error("Can't load option keys", e);
		}
	}

	public static Map<String, Option> getOptionMap() {
		return Collections.unmodifiableMap(OPTION_MAP);
	}

	private HDTOptionsKeys() {
	}

	public static class OptionValue {
		private final String value;
		private final Value valueInfo;

		private OptionValue(String value, Value valueInfo) {
			this.value = value;
			this.valueInfo = valueInfo;
		}

		public String getValue() {
			return value;
		}

		public Value getValueInfo() {
			return valueInfo;
		}
	}

	public static class Option {
		private final String key;
		private final Key keyInfo;
		private final List<OptionValue> values = new ArrayList<>();

		private Option(String key, Key keyInfo) {
			this.key = key;
			this.keyInfo = keyInfo;
		}

		public String getKey() {
			return key;
		}

		public Key getKeyInfo() {
			return keyInfo;
		}

		public List<OptionValue> getValues() {
			return Collections.unmodifiableList(values);
		}
	}
}
