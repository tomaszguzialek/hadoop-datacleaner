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
package org.eobjects.hadoopdatacleaner.configuration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eobjects.analyzer.beans.api.Analyzer;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfigurationImpl;
import org.eobjects.analyzer.connection.Datastore;
import org.eobjects.analyzer.connection.DatastoreCatalog;
import org.eobjects.analyzer.connection.DatastoreCatalogImpl;
import org.eobjects.analyzer.connection.PojoDatastore;
import org.eobjects.analyzer.descriptors.ClasspathScanDescriptorProvider;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.job.AnalyzerJob;
import org.eobjects.analyzer.job.JaxbJobReader;
import org.eobjects.analyzer.job.JaxbJobWriter;
import org.eobjects.analyzer.lifecycle.LifeCycleHelper;
import org.eobjects.analyzer.util.LabelUtils;
import org.eobjects.analyzer.util.ReflectionUtils;
import org.apache.metamodel.pojo.ArrayTableDataProvider;
import org.apache.metamodel.pojo.TableDataProvider;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;

public class ConfigurationSerializer {

	public static AnalyzerBeansConfiguration deserializeAnalyzerBeansDatastores(
			String datastoresInput) {

		Map<String, List<TableDataProvider<?>>> tablesMap = new HashMap<String, List<TableDataProvider<?>>>();

		String[] datastoreLines = datastoresInput.split("\n");
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
			String mapKey = datastoreName + "%#%" + schemaName;
			if (!tablesMap.containsKey(mapKey)) {
				tablesMap.put(mapKey, tableDataProviders);
			} else {
				tablesMap.get(mapKey).addAll(tableDataProviders);
			}

		}

		List<Datastore> datastores = new ArrayList<Datastore>();
		for (Map.Entry<String, List<TableDataProvider<?>>> mapEntry : tablesMap
				.entrySet()) {
			String mapKey = mapEntry.getKey();
			String[] split = mapKey.split("%#%");
			String datastoreName = split[0];
			String schemaName = split[1];
			List<TableDataProvider<?>> tableDataProviders = mapEntry.getValue();
			Datastore datastore = new PojoDatastore(datastoreName, schemaName,
					tableDataProviders);
			datastores.add(datastore);
		}

		DatastoreCatalog datastoreCatalog = new DatastoreCatalogImpl(datastores);

		ClasspathScanDescriptorProvider descriptorProvider = new ClasspathScanDescriptorProvider();
		descriptorProvider.scanPackage("org.eobjects", true);

		return new AnalyzerBeansConfigurationImpl().replace(datastoreCatalog)
				.replace(descriptorProvider);
	}

	public static AnalysisJob deserializeAnalysisJobFromXml(
			String analysisJobXml,
			AnalyzerBeansConfiguration analyzerBeansConfiguration) {
		JaxbJobReader jobReader = new JaxbJobReader(analyzerBeansConfiguration);
		return jobReader.read(new ByteArrayInputStream(analysisJobXml
				.getBytes()));
	}

	public static String serializeAnalyzerBeansConfigurationDataStores(
			AnalyzerBeansConfiguration analyzerBeansConfiguration) {

		StringBuilder datastoresOutputBuilder = new StringBuilder();

		DatastoreCatalog datastoreCatalog = analyzerBeansConfiguration
				.getDatastoreCatalog();
		for (String datastoreName : analyzerBeansConfiguration
				.getDatastoreCatalog().getDatastoreNames()) {
			Datastore datastore = datastoreCatalog.getDatastore(datastoreName);
			Schema schema = datastore.openConnection().getDataContext()
					.getDefaultSchema();
			for (Table table : schema.getTables()) {
				datastoresOutputBuilder.append(datastoreName);
				datastoresOutputBuilder.append(",");
				datastoresOutputBuilder.append(schema.getName());
				datastoresOutputBuilder.append(",");
				datastoresOutputBuilder.append(table.getName());
				datastoresOutputBuilder.append(",");
				String[] columnNames = table.getColumnNames();
				for (int i = 0; i < columnNames.length; i++) {
					datastoresOutputBuilder.append(columnNames[i]);
					if (i == columnNames.length - 1)
						datastoresOutputBuilder.append("\n");
					else
						datastoresOutputBuilder.append(",");
				}
			}
		}

		return datastoresOutputBuilder.toString();
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

	public static Analyzer<?> initializeAnalyzer(String analyzerKey,
			AnalyzerBeansConfiguration analyzerBeansConfiguration,
			AnalysisJob analysisJob) {

		AnalyzerJob analyzerJob = null;

		for (AnalyzerJob analyzerJobIter : analysisJob.getAnalyzerJobs()) {
			String analyzerLabel = LabelUtils.getLabel(analyzerJobIter);
			if (analyzerLabel.equals(analyzerKey.toString())) {
				analyzerJob = analyzerJobIter;
				break;
			}
		}

		if (analyzerJob != null) {
			Analyzer<?> analyzer = ReflectionUtils.newInstance(analyzerJob
					.getDescriptor().getComponentClass());
			LifeCycleHelper lifeCycleHelper = new LifeCycleHelper(
					analyzerBeansConfiguration.getInjectionManager(analysisJob),
					true);
			lifeCycleHelper.assignConfiguredProperties(
					analyzerJob.getDescriptor(), analyzer,
					analyzerJob.getConfiguration());
			lifeCycleHelper.assignProvidedProperties(
					analyzerJob.getDescriptor(), analyzer);
			lifeCycleHelper.validate(analyzerJob.getDescriptor(), analyzer);
			lifeCycleHelper.initialize(analyzerJob.getDescriptor(), analyzer);
			return analyzer;
		} else {
			throw new IllegalArgumentException("Specified analyzer key: "
					+ analyzerKey + "has not been found in the analysis job.");
		}
	}

}