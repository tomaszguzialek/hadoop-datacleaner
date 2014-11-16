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
package org.eobjects.hadoopdatacleaner.tools;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.hadoopdatacleaner.configuration.AnalyzerBeansConfigurationHelper;
import org.xml.sax.SAXException;

public class HadoopDataCleanerTool extends Configured {

	public static final String ANALYSIS_JOB_XML_KEY = "analysis.job.xml";

	protected AnalyzerBeansConfiguration analyzerBeansConfiguration;

	protected String analysisJobXml;

	public HadoopDataCleanerTool(String analysisJobXml) throws IOException,
			XPathExpressionException, ParserConfigurationException,
			SAXException {
		this.analyzerBeansConfiguration = AnalyzerBeansConfigurationHelper
				.build(analysisJobXml);
		this.analysisJobXml = analysisJobXml;
	}

	public HadoopDataCleanerTool(Configuration conf) {
		super(conf);
	}

}