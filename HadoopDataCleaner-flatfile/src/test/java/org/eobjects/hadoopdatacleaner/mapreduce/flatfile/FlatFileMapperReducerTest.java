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
package org.eobjects.hadoopdatacleaner.mapreduce.flatfile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.eobjects.hadoopdatacleaner.configuration.AnalyzerBeansConfigurationHelper;
import org.eobjects.hadoopdatacleaner.tools.FlatFileTool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

public class FlatFileMapperReducerTest {

	MapDriver<LongWritable, Text, Text, SortedMapWritable> mapDriver;
	ReduceDriver<Text, SortedMapWritable, NullWritable, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, SortedMapWritable, NullWritable, Text> mapReduceDriver;

	@Before
	public void setUp() throws XPathExpressionException,
			ParserConfigurationException, SAXException, IOException {
		String inputTableName = "contactdata.txt";
		String outputTableName = this.getClass().getSimpleName() + ".txt";
		
		String analysisJobXml = FileUtils.readFileToString(new File(
				"src/test/resources/contactdata.analysis.xml"));

		FlatFileMapper flatFileMapper = new FlatFileMapper();
		FlatFileReducer flatFileReducer = new FlatFileReducer();

		mapDriver = MapDriver.newMapDriver(flatFileMapper);
		mapDriver.getConfiguration().set(FlatFileTool.ANALYSIS_JOB_XML_KEY,
				analysisJobXml);
		mapDriver.getConfiguration().set(FlatFileTool.INPUT_TABLE_NAME_KEY, inputTableName);
		mapDriver.getConfiguration().set(FlatFileTool.OUTPUT_TABLE_NAME_KEY, outputTableName);

		reduceDriver = ReduceDriver.newReduceDriver(flatFileReducer);
		reduceDriver.getConfiguration().set(FlatFileTool.ANALYSIS_JOB_XML_KEY,
				analysisJobXml);
		reduceDriver.getConfiguration().set(FlatFileTool.INPUT_TABLE_NAME_KEY, inputTableName);
		reduceDriver.getConfiguration().set(FlatFileTool.OUTPUT_TABLE_NAME_KEY, outputTableName);

		mapReduceDriver = MapReduceDriver.newMapReduceDriver(flatFileMapper,
				flatFileReducer);
	}

	@Test
	public void testMapper() throws IOException {
		SortedMapWritable expectedHumanInference = new SortedMapWritable();
		expectedHumanInference.put(new Text("Company"), new Text(
				"Human Inference"));
		expectedHumanInference.put(new Text("AddressLine1"), new Text(
				"Utrechtseweg 310"));
		expectedHumanInference.put(new Text("AddressLine2"), new Text(
				"6812 AR Arnhem"));
		expectedHumanInference.put(new Text("AddressLine3"), new Text(
				"6812 AR Arnhem"));
		expectedHumanInference.put(new Text("AddressLine4"), new Text(""));
		expectedHumanInference.put(new Text("Country"), new Text("NLD"));
		expectedHumanInference.put(new Text("Phone"), new Text(
				"+31 (0)26 3550655"));

		mapDriver
				.withInput(
						new LongWritable(0),
						new Text("Company;AddressLine1;AddressLine2;"
								+ "AddressLine3;AddressLine4;Country;Phone"))
				.withInput(new LongWritable(44), new Text("Poland;PL;POL;616;"));

		List<Pair<Text, SortedMapWritable>> actualOutputs = mapDriver.run();

		Assert.assertEquals(2, actualOutputs.size());

		Pair<Text, SortedMapWritable> actualOutputPoland = actualOutputs.get(0);
		actualOutputPoland.getSecond().containsValue("Utrechtseweg 310");
	}

	@Test
	public void testReducerHeader() throws IOException {
		List<SortedMapWritable> rows = new ArrayList<SortedMapWritable>();

		SortedMapWritable header = new SortedMapWritable();
		header.put(new Text("ISO 3166-2_ISO 3166-3"), new Text(
				"ISO 3166-2_ISO 3166-3"));
		header.put(new Text("Country name"), new Text("Country name"));
		header.put(new Text("ISO 3166-2"), new Text("ISO 3166-2"));
		header.put(new Text("ISO 3166-3"), new Text("ISO 3166-3"));
		header.put(new Text("ISO Numeric"), new Text("ISO Numeric"));
		header.put(new Text("Linked to country"), new Text("Linked to country"));
		header.put(new Text("Synonym1"), new Text("Synonym1"));
		header.put(new Text("Synonym2"), new Text("Synonym2"));
		header.put(new Text("Synonym3"), new Text("Synonym3"));
		rows.add(header);

		reduceDriver.withInput(new Text("Value distribution (Country name)"),
				rows);
		reduceDriver
				.withOutput(
						NullWritable.get(),
						new Text(
								"Country name;ISO 3166-2;ISO 3166-2_ISO 3166-3;ISO 3166-3;ISO Numeric;Linked to country;Synonym1;Synonym2;Synonym3"));
		reduceDriver.runTest();
	}

	@Test
	public void testReducerPoland() throws IOException {
		List<SortedMapWritable> rows = new ArrayList<SortedMapWritable>();

		SortedMapWritable poland = new SortedMapWritable();
		poland.put(new Text("Country name"), new Text("Poland"));
		poland.put(new Text("ISO 3166-2"), new Text("PL"));
		poland.put(new Text("ISO 3166-3"), new Text("POL"));
		rows.add(poland);

		reduceDriver.withInput(new Text("Value distribution (Country name)"),
				rows);
		reduceDriver.withOutput(NullWritable.get(), new Text("Poland;PL;POL"));
		reduceDriver.runTest();

	}

}
