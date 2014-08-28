/**
 * DataCleaner (community edition)
 * Copyright (C) 2013 Human Inference

 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.hadoopdatacleaner.mapreduce.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.eobjects.analyzer.beans.StringAnalyzer;
import org.eobjects.analyzer.beans.transform.ConcatenatorTransformer;
import org.eobjects.analyzer.beans.transform.TokenizerTransformer;
import org.eobjects.analyzer.beans.valuedist.ValueDistributionAnalyzer;
import org.eobjects.analyzer.beans.writers.InsertIntoTableAnalyzer;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfigurationImpl;
import org.eobjects.analyzer.connection.Datastore;
import org.eobjects.analyzer.connection.DatastoreCatalog;
import org.eobjects.analyzer.connection.DatastoreCatalogImpl;
import org.eobjects.analyzer.connection.PojoDatastore;
import org.eobjects.analyzer.descriptors.Descriptors;
import org.eobjects.analyzer.descriptors.SimpleDescriptorProvider;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.job.builder.AnalysisJobBuilder;
import org.eobjects.analyzer.job.builder.AnalyzerJobBuilder;
import org.eobjects.analyzer.job.builder.TransformerJobBuilder;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;
import org.eobjects.hadoopdatacleaner.tools.HBaseTool;
import org.apache.metamodel.pojo.ArrayTableDataProvider;
import org.apache.metamodel.pojo.TableDataProvider;
import org.apache.metamodel.util.SimpleTableDef;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HBaseTableMapperTest {

	MapDriver<ImmutableBytesWritable, Result, Text, SortedMapWritable> mapDriver;

	@Before
	public void setUp() {
		AnalyzerBeansConfiguration analyzerBeansConfiguration = buildAnalyzerBeansConfiguration();
		AnalysisJob analysisJob = buildAnalysisJob(analyzerBeansConfiguration);
		String analyzerBeansConfigurationDatastores = ConfigurationSerializer
				.serializeAnalyzerBeansConfigurationDataStores(analyzerBeansConfiguration);
		String analysisJobXml = ConfigurationSerializer
				.serializeAnalysisJobToXml(analyzerBeansConfiguration,
						analysisJob);
		HBaseTableMapper hBaseTableMapper = new HBaseTableMapper();
		mapDriver = MapDriver.newMapDriver(hBaseTableMapper);
		mapDriver
				.getConfiguration()
				.set("io.serializations",
						"org.apache.hadoop.hbase.mapreduce.ResultSerialization,"
						+ "org.apache.hadoop.hbase.mapreduce.KeyValueSerialization,"
						+ "org.apache.hadoop.hbase.mapreduce.MutationSerialization,"
						+ "org.apache.hadoop.io.serializer.JavaSerialization,"
						+ "org.apache.hadoop.io.serializer.WritableSerialization");
		mapDriver.getConfiguration().set(
				HBaseTool.ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY,
				analyzerBeansConfigurationDatastores);
		mapDriver.getConfiguration().set(HBaseTool.ANALYSIS_JOB_XML_KEY,
				analysisJobXml);
	}

	@Test
	public void testDenmark() throws IOException {
		ImmutableBytesWritable inputKey = new ImmutableBytesWritable(
				Bytes.toBytes("Denmark"));

		List<Cell> cells = new ArrayList<Cell>();
		Cell cell = new KeyValue(Bytes.toBytes("Denmark"),
				Bytes.toBytes("mainFamily"), Bytes.toBytes("country_name"),
				Bytes.toBytes("Denmark"));
		cells.add(cell);
		cell = new KeyValue(Bytes.toBytes("Denmark"),
				Bytes.toBytes("mainFamily"), Bytes.toBytes("iso2"),
				Bytes.toBytes("DK"));
		cells.add(cell);
		cell = new KeyValue(Bytes.toBytes("Denmark"),
				Bytes.toBytes("mainFamily"), Bytes.toBytes("iso3"),
				Bytes.toBytes("DNK"));
		cells.add(cell);
		Result inputResult = Result.create(cells);

		SortedMapWritable expectedOutput = new SortedMapWritable();
		expectedOutput.put(new Text("mainFamily:country_name"), new Text(
				"Denmark"));
		expectedOutput.put(new Text("mainFamily:iso2"), new Text("DK"));
		expectedOutput
				.put(new Text("mainFamily:iso2_iso3"), new Text("DK_DNK"));
		expectedOutput.put(new Text("mainFamily:iso3"), new Text("DNK"));

		String expectedAnalyzerKey1 = "Value distribution (mainFamily:country_name)";
		String expectedAnalyzerKey2 = "Value distribution (mainFamily:iso2)";

		mapDriver.withInput(inputKey, inputResult);
		List<Pair<Text, SortedMapWritable>> actualOutputs = mapDriver.run();
		Assert.assertEquals(actualOutputs.size(), 2);
		Pair<Text, SortedMapWritable> actualOutput1 = actualOutputs.get(0);
		Pair<Text, SortedMapWritable> actualOutput2 = actualOutputs.get(1);

		Assert.assertEquals(expectedAnalyzerKey1, actualOutput1.getFirst()
				.toString());
		Assert.assertEquals(expectedAnalyzerKey2, actualOutput2.getFirst()
				.toString());
		for (@SuppressWarnings("rawtypes")
		Map.Entry<WritableComparable, Writable> mapEntry : expectedOutput
				.entrySet()) {
			Text expectedColumnName = (Text) mapEntry.getKey();
			Text expectedColumnValue = (Text) mapEntry.getValue();

			Assert.assertTrue(actualOutput1.getSecond().containsKey(
					expectedColumnName));
			Assert.assertEquals(expectedColumnValue, actualOutput1.getSecond()
					.get(expectedColumnName));

			Assert.assertTrue(actualOutput2.getSecond().containsKey(
					expectedColumnName));
			Assert.assertEquals(expectedColumnValue, actualOutput2.getSecond()
					.get(expectedColumnName));
		}
	}

	public static AnalyzerBeansConfiguration buildAnalyzerBeansConfiguration() {
		List<TableDataProvider<?>> tableDataProviders = new ArrayList<TableDataProvider<?>>();
		SimpleTableDef tableDef1 = new SimpleTableDef("countrycodes",
				new String[] { "mainFamily:country_name", "mainFamily:iso2",
						"mainFamily:iso3" });
		SimpleTableDef tableDef2 = new SimpleTableDef("countrycodes_output",
				new String[] { "mainFamily:country_name", "mainFamily:iso2",
						"mainFamily:iso3" });
		tableDataProviders.add(new ArrayTableDataProvider(tableDef1,
				new ArrayList<Object[]>()));
		tableDataProviders.add(new ArrayTableDataProvider(tableDef2,
				new ArrayList<Object[]>()));
		Datastore datastore = new PojoDatastore("countrycodes_hbase",
				"countrycodes_schema", tableDataProviders);

		DatastoreCatalog datastoreCatalog = new DatastoreCatalogImpl(datastore);

		SimpleDescriptorProvider descriptorProvider = new SimpleDescriptorProvider(
				true);
		descriptorProvider.addTransformerBeanDescriptor(Descriptors
				.ofTransformer(ConcatenatorTransformer.class));
		descriptorProvider.addTransformerBeanDescriptor(Descriptors
				.ofTransformer(TokenizerTransformer.class));
		descriptorProvider.addAnalyzerBeanDescriptor(Descriptors
				.ofAnalyzer(InsertIntoTableAnalyzer.class));
		descriptorProvider.addAnalyzerBeanDescriptor(Descriptors
				.ofAnalyzer(StringAnalyzer.class));

		return new AnalyzerBeansConfigurationImpl().replace(datastoreCatalog)
				.replace(descriptorProvider);
	}

	public static AnalysisJob buildAnalysisJob(
			AnalyzerBeansConfiguration configuration) {
		AnalysisJobBuilder ajb = new AnalysisJobBuilder(configuration);
		try {
			ajb.setDatastore("countrycodes_hbase");

			ajb.addSourceColumns(
					"countrycodes_schema.countrycodes.mainFamily:country_name",
					"countrycodes_schema.countrycodes.mainFamily:iso2",
					"countrycodes_schema.countrycodes.mainFamily:iso3");

			TransformerJobBuilder<ConcatenatorTransformer> concatenator = ajb
					.addTransformer(ConcatenatorTransformer.class);
			concatenator.addInputColumns(ajb
					.getSourceColumnByName("mainFamily:iso2"));
			concatenator.addInputColumns(ajb
					.getSourceColumnByName("mainFamily:iso3"));
			concatenator.setConfiguredProperty("Separator", "_");
			concatenator.getOutputColumns().get(0)
					.setName("mainFamily:iso2_iso3");

			AnalyzerJobBuilder<ValueDistributionAnalyzer> valueDistributionAnalyzer = ajb
					.addAnalyzer(ValueDistributionAnalyzer.class);
			valueDistributionAnalyzer.addInputColumn(ajb
					.getSourceColumnByName("mainFamily:country_name"));

			AnalyzerJobBuilder<ValueDistributionAnalyzer> valueDistributionAnalyzer2 = ajb
					.addAnalyzer(ValueDistributionAnalyzer.class);
			valueDistributionAnalyzer2.addInputColumn(ajb
					.getSourceColumnByName("mainFamily:iso2"));

			return ajb.toAnalysisJob();
		} finally {
			ajb.close();
		}
	}

}
