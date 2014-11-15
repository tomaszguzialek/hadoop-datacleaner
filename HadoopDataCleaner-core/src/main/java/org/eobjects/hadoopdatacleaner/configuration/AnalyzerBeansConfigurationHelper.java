package org.eobjects.hadoopdatacleaner.configuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.metamodel.pojo.ArrayTableDataProvider;
import org.apache.metamodel.pojo.TableDataProvider;
import org.apache.metamodel.util.SimpleTableDef;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfigurationImpl;
import org.eobjects.analyzer.connection.Datastore;
import org.eobjects.analyzer.connection.DatastoreCatalog;
import org.eobjects.analyzer.connection.DatastoreCatalogImpl;
import org.eobjects.analyzer.connection.PojoDatastore;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.descriptors.ClasspathScanDescriptorProvider;
import org.eobjects.analyzer.descriptors.DescriptorProvider;
import org.eobjects.analyzer.job.AnalysisJob;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class AnalyzerBeansConfigurationHelper {

	private static final String[] DEFAULT_PACKAGES = new String[] { "org.eobjects" };

	public static AnalyzerBeansConfiguration build(AnalysisJob analysisJob) {
		return build(analysisJob, getDefaultDescriptorProvider());
	}

	public static AnalyzerBeansConfiguration build(String analysisJobXml)
			throws XPathExpressionException, ParserConfigurationException,
			SAXException, IOException {
		return build(analysisJobXml, getDefaultDescriptorProvider());
	}

	public static AnalyzerBeansConfiguration build(String analysisJobXml,
			DescriptorProvider descriptorProvider)
			throws XPathExpressionException, ParserConfigurationException,
			SAXException, IOException {

		String datastoreName = getDatastoreName(analysisJobXml);
		String[] sourceColumns = getSourceColumns(analysisJobXml);

		return build(datastoreName, sourceColumns, descriptorProvider);
	}

	public static AnalyzerBeansConfiguration build(AnalysisJob analysisJob,
			DescriptorProvider descriptorProvider) {
		String datastoreName = analysisJob.getDatastore().getName();
		List<InputColumn<?>> sourceColumns = analysisJob.getSourceColumns();

		String[] columnNames = new String[sourceColumns.size()];
		int i = 0;
		for (InputColumn<?> inputColumn : sourceColumns) {
			columnNames[i++] = inputColumn.getName();
		}

		return build(datastoreName, columnNames, descriptorProvider);
	}

	public static AnalyzerBeansConfiguration build(String datastoreName,
			String[] columnNames) {
		return build(datastoreName, columnNames, getDefaultDescriptorProvider());
	}

	public static AnalyzerBeansConfiguration build(String datastoreName,
			String[] columnNames, DescriptorProvider descriptorProvider) {
		List<TableDataProvider<?>> tableDataProviders = new ArrayList<TableDataProvider<?>>();
		SimpleTableDef inputTableDef = new SimpleTableDef(datastoreName,
				columnNames);
		tableDataProviders.add(new ArrayTableDataProvider(inputTableDef,
				new ArrayList<Object[]>()));
		Datastore datastore = new PojoDatastore(datastoreName, datastoreName,
				tableDataProviders);

		DatastoreCatalog datastoreCatalog = new DatastoreCatalogImpl(datastore);

		return new AnalyzerBeansConfigurationImpl().replace(datastoreCatalog)
				.replace(descriptorProvider);
	}

	private static String getDatastoreName(String analysisJobXml)
			throws ParserConfigurationException, SAXException, IOException,
			XPathExpressionException {
		DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance()
				.newDocumentBuilder();
		Document document = documentBuilder.parse(new ByteArrayInputStream(
				analysisJobXml.getBytes("utf-8")));

		XPath xPath = XPathFactory.newInstance().newXPath();
		XPathExpression xPathExpression = xPath
				.compile("/job/source/data-context[@ref]");

		Node dataContextNode = (Node) xPathExpression.evaluate(document,
				XPathConstants.NODE);
		Node refAttributeNode = dataContextNode.getAttributes().getNamedItem(
				"ref");
		String datastoreName = refAttributeNode.getNodeValue();
		return datastoreName;
	}

	private static String[] getSourceColumns(String analysisJobXml)
			throws XPathExpressionException, UnsupportedEncodingException,
			SAXException, IOException, ParserConfigurationException {
		DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance()
				.newDocumentBuilder();
		Document document = documentBuilder.parse(new ByteArrayInputStream(
				analysisJobXml.getBytes("utf-8")));

		XPath xPath = XPathFactory.newInstance().newXPath();
		XPathExpression xPathExpression = xPath
				.compile("/job/source/columns/column[@path]");

		NodeList columnNodes = (NodeList) xPathExpression.evaluate(document,
				XPathConstants.NODESET);
		
		List<String> columnNames = new ArrayList<String>();
		for (int i = 0; i < columnNodes.getLength(); i++) {
			Node columnNode = columnNodes.item(i);
			Node pathAttributeNode = columnNode.getAttributes().getNamedItem("path");
			columnNames.add(pathAttributeNode.getNodeValue());
		}
		return columnNames.toArray(new String[columnNodes.getLength()]);
	}

	private static ClasspathScanDescriptorProvider getDefaultDescriptorProvider() {
		ClasspathScanDescriptorProvider descriptorProvider = new ClasspathScanDescriptorProvider();
		for (String packageName : DEFAULT_PACKAGES) {
			descriptorProvider.scanPackage(packageName, true);
		}
		return descriptorProvider;
	}

}
