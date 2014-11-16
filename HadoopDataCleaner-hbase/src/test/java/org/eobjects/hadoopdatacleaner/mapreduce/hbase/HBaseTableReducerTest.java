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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.metamodel.pojo.ArrayTableDataProvider;
import org.apache.metamodel.pojo.TableDataProvider;
import org.apache.metamodel.util.SimpleTableDef;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTableReducerTest {

	ReduceDriver<Text, SortedMapWritable, NullWritable, Mutation> reduceDriver;

	@SuppressWarnings("unused")
	private static final Logger logger = LoggerFactory
			.getLogger(HBaseTableReducer.class);

	@Before
	public void setUp() {
		AnalyzerBeansConfiguration analyzerBeansConfiguration = buildAnalyzerBeansConfiguration();
		AnalysisJob analysisJob = buildAnalysisJob(analyzerBeansConfiguration);
		String analysisJobXml = ConfigurationSerializer
				.serializeAnalysisJobToXml(analyzerBeansConfiguration,
						analysisJob);
		HBaseTableReducer hBaseTableReducer = new HBaseTableReducer();
		reduceDriver = ReduceDriver.newReduceDriver(hBaseTableReducer);
		reduceDriver
				.getConfiguration()
				.set("io.serializations",
						"org.apache.hadoop.hbase.mapreduce.ResultSerialization,"
								+ "org.apache.hadoop.hbase.mapreduce.KeyValueSerialization,"
								+ "org.apache.hadoop.hbase.mapreduce.MutationSerialization,"
								+ "org.apache.hadoop.io.serializer.JavaSerialization,"
								+ "org.apache.hadoop.io.serializer.WritableSerialization");
		reduceDriver.getConfiguration().set(HBaseTool.ANALYSIS_JOB_XML_KEY,
				analysisJobXml);
	}

	@Test
	public void testReducer() throws IOException {
		List<SortedMapWritable> inputRows = new ArrayList<SortedMapWritable>();
		SortedMapWritable inputRow = new SortedMapWritable();
		inputRow.put(new Text("mainFamily:country_name"), new Text("Denmark"));
		inputRow.put(new Text("mainFamily:iso2"), new Text("DK"));
		inputRow.put(new Text("mainFamily:iso2_iso3"), new Text("DK_DNK"));
		inputRow.put(new Text("mainFamily:iso3"), new Text("DNK"));
		inputRows.add(inputRow);

		inputRow = new SortedMapWritable();
		inputRow.put(new Text("mainFamily:country_name"), new Text("Poland"));
		inputRow.put(new Text("mainFamily:iso2"), new Text("PL"));
		inputRow.put(new Text("mainFamily:iso2_iso3"), new Text("PL_POL"));
		inputRow.put(new Text("mainFamily:iso3"), new Text("POL"));
		inputRows.add(inputRow);

		String inputAnalyzerKey1 = "Value distribution (countrycodes_schema.countrycodes.mainFamily:country_name)";

		reduceDriver.withInput(new Text(inputAnalyzerKey1), inputRows);
		List<Pair<NullWritable, Mutation>> actualOutputs = reduceDriver.run();
		Assert.assertEquals(2, actualOutputs.size());

		Pair<NullWritable, Mutation> actualOutput1 = actualOutputs.get(0);
		Put actualPut1 = (Put) actualOutput1.getSecond();
		List<Cell> keyValues = actualPut1.get(Bytes.toBytes("mainFamily"),
				Bytes.toBytes("country_name"));
		Assert.assertEquals(1, keyValues.size());
		Cell cell = keyValues.get(0);
		System.out.println("Value: "
				+ Bytes.toString(CellUtil.cloneValue(cell)));
		Assert.assertEquals("Denmark",
				Bytes.toString(CellUtil.cloneValue(cell)));

		Pair<NullWritable, Mutation> actualOutput2 = actualOutputs.get(1);
		Put actualPut2 = (Put) actualOutput2.getSecond();
		keyValues = actualPut2.get(Bytes.toBytes("mainFamily"),
				Bytes.toBytes("country_name"));
		Assert.assertEquals(1, keyValues.size());
		cell = keyValues.get(0);
		System.out.println("Value: "
				+ Bytes.toString(CellUtil.cloneValue(cell)));
		Assert.assertEquals("Poland", Bytes.toString(CellUtil.cloneValue(cell)));
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
