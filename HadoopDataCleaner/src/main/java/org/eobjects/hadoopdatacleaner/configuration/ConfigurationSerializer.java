package org.eobjects.hadoopdatacleaner.configuration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.eobjects.analyzer.beans.StringAnalyzer;
import org.eobjects.analyzer.beans.transform.ConcatenatorTransformer;
import org.eobjects.analyzer.beans.transform.TokenizerTransformer;
import org.eobjects.analyzer.beans.valuedist.ValueDistributionAnalyzer;
import org.eobjects.analyzer.beans.writers.InsertIntoTableAnalyzer;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfigurationImpl;
import org.eobjects.analyzer.connection.CsvDatastore;
import org.eobjects.analyzer.connection.Datastore;
import org.eobjects.analyzer.connection.DatastoreCatalog;
import org.eobjects.analyzer.connection.DatastoreCatalogImpl;
import org.eobjects.analyzer.connection.PojoDatastore;
import org.eobjects.analyzer.descriptors.Descriptors;
import org.eobjects.analyzer.descriptors.SimpleDescriptorProvider;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.job.JaxbJobReader;
import org.eobjects.analyzer.job.JaxbJobWriter;
import org.eobjects.analyzer.util.SchemaNavigator;
import org.eobjects.metamodel.pojo.ArrayTableDataProvider;
import org.eobjects.metamodel.pojo.TableDataProvider;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.SimpleTableDef;

public class ConfigurationSerializer {

	public static AnalyzerBeansConfiguration deserializeDatastoresFromCsv(
			String datastoresCsvLines) {

		List<Datastore> datastores = new ArrayList<Datastore>();

		String[] datastoreLines = datastoresCsvLines.split("\n");
		for (String datastoreLine : datastoreLines) {
			String[] items = datastoreLine.split(",");
			String datastoreName = items[0];
			String schemaName = items[1];
			String tableName = items[2];
			String[] columnNames = new String[items.length];
			for (int i = 3; i < items.length; i++) {
				columnNames[i - 3] = items[i];
			}

			List<TableDataProvider<?>> tableDataProviders = new ArrayList<TableDataProvider<?>>();
			SimpleTableDef tableDef = new SimpleTableDef(tableName, columnNames);
			tableDataProviders.add(new ArrayTableDataProvider(tableDef,
					new ArrayList<Object[]>()));
			Datastore datastore = new PojoDatastore(datastoreName,
						schemaName, tableDataProviders);
			datastores.add(datastore);
		}

		DatastoreCatalog datastoreCatalog = new DatastoreCatalogImpl(datastores);
		
		// TODO: Make class path scanning work.
//		ClasspathScanDescriptorProvider descriptorProvider = new ClasspathScanDescriptorProvider();
//		descriptorProvider.scanPackage("org.eobjects");
		SimpleDescriptorProvider descriptorProvider = new SimpleDescriptorProvider(true);		
		descriptorProvider.addTransformerBeanDescriptor(Descriptors
				.ofTransformer(ConcatenatorTransformer.class));
		descriptorProvider.addTransformerBeanDescriptor(Descriptors
				.ofTransformer(TokenizerTransformer.class));
		descriptorProvider.addAnalyzerBeanDescriptor(Descriptors
				.ofAnalyzer(InsertIntoTableAnalyzer.class));
		descriptorProvider.addAnalyzerBeanDescriptor(Descriptors
				.ofAnalyzer(StringAnalyzer.class));
		descriptorProvider.addAnalyzerBeanDescriptor(Descriptors
				.ofAnalyzer(ValueDistributionAnalyzer.class));
		
		return new AnalyzerBeansConfigurationImpl().replace(datastoreCatalog).replace(descriptorProvider);
	}

	public static AnalysisJob deserializeAnalysisJobFromXml(
			String analysisJobXml,
			AnalyzerBeansConfiguration analyzerBeansConfiguration) {
		JaxbJobReader jobReader = new JaxbJobReader(analyzerBeansConfiguration);
		return jobReader.read(new ByteArrayInputStream(analysisJobXml
				.getBytes()));
	}

	public static String serializeAnalyzerBeansConfigurationToCsv(
			AnalyzerBeansConfiguration analyzerBeansConfiguration) {

		StringBuilder datastoresCsvBuilder = new StringBuilder();

		DatastoreCatalog datastoreCatalog = analyzerBeansConfiguration
				.getDatastoreCatalog();
		for (String datastoreName : analyzerBeansConfiguration
				.getDatastoreCatalog().getDatastoreNames()) {
			Datastore datastore = datastoreCatalog.getDatastore(datastoreName);
			if (datastore instanceof CsvDatastore) {
				Schema schema = datastore.openConnection().getDataContext()
						.getDefaultSchema();
				for (Table table : schema.getTables()) {
					datastoresCsvBuilder.append(datastoreName);
					datastoresCsvBuilder.append(",");
					datastoresCsvBuilder.append(schema.getName());
					datastoresCsvBuilder.append(",");
					datastoresCsvBuilder.append(table.getName());
					datastoresCsvBuilder.append(",");
					String[] columnNames = table.getColumnNames();
					for (int i = 0; i < columnNames.length; i++) {
						datastoresCsvBuilder.append(columnNames[i]);
						if (i == columnNames.length - 1)
							datastoresCsvBuilder.append("\n");
						else
							datastoresCsvBuilder.append(",");
					}
				}
			} else {
				SchemaNavigator schemaNavigator = datastore.openConnection()
						.getSchemaNavigator();
				for (Schema schema : schemaNavigator.getSchemas()) {
					for (Table table : schema.getTables()) {
						datastoresCsvBuilder.append(datastoreName);
						datastoresCsvBuilder.append(",");
						datastoresCsvBuilder.append(schema.getName());
						datastoresCsvBuilder.append(",");
						datastoresCsvBuilder.append(table.getName());
						datastoresCsvBuilder.append(",");
						String[] columnNames = table.getColumnNames();
						for (int i = 0; i < columnNames.length; i++) {
							datastoresCsvBuilder.append(columnNames[i]);
							if (i == columnNames.length - 1)
								datastoresCsvBuilder.append("\n");
							else
								datastoresCsvBuilder.append(",");
						}
					}
				}

			}
		}

		return datastoresCsvBuilder.toString();
	}
	
	public static String serializeAnalysisJobToXml(
			AnalyzerBeansConfiguration analyzerBeansConfiguration,
			AnalysisJob analysisJob) {
		JaxbJobWriter jaxbJobWriter = new JaxbJobWriter(
				analyzerBeansConfiguration);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		jaxbJobWriter.write(analysisJob, outputStream);
		String analysisJobXml = null;
		try {
			analysisJobXml = outputStream.toString("UTF8");
		} catch (UnsupportedEncodingException e) {
			// This should never happen.
			e.printStackTrace();
		}
		return analysisJobXml;
	}

}