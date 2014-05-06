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
package org.eobjects.hadoopdatacleaner.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.eobjects.analyzer.beans.filter.ValidationCategory;
import org.eobjects.analyzer.beans.valuedist.ValueDistributionAnalyzer;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.data.MockInputColumn;
import org.eobjects.analyzer.data.MockInputRow;
import org.eobjects.analyzer.descriptors.AnalyzerBeanDescriptor;
import org.eobjects.analyzer.descriptors.Descriptors;
import org.eobjects.analyzer.job.AnalyzerJob;
import org.eobjects.analyzer.job.BeanConfiguration;
import org.eobjects.analyzer.job.ImmutableAnalyzerJob;
import org.eobjects.analyzer.job.ImmutableBeanConfiguration;
import org.eobjects.analyzer.job.ImmutableFilterJob;
import org.eobjects.analyzer.job.ImmutableFilterOutcome;
import org.eobjects.analyzer.job.runner.ConsumeRowResult;
import org.eobjects.analyzer.job.runner.OutcomeSink;
import org.eobjects.analyzer.job.runner.OutcomeSinkImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MapperEmitterTest {

    private final List<SortedMapWritable> emitList = new ArrayList<SortedMapWritable>();
    private MapperEmitter mapperEmitter = null;

    @Before
    public void setUp() {
        this.mapperEmitter = new MapperEmitter(new MapperEmitter.Callback() {

            public void write(Text text, SortedMapWritable row) throws IOException, InterruptedException {
                emitList.add(row);

            }
        });

    }

    @Test
    public void testEmit() throws IOException, InterruptedException {
        List<InputRow> rows = new ArrayList<InputRow>();
        MockInputRow row = new MockInputRow();
        row.put(new MockInputColumn<String>("iso2"), "PL");
        rows.add(row);
        
        List<OutcomeSink> outcomeSinks = new ArrayList<OutcomeSink>(); 
        OutcomeSink outcomeSink = new OutcomeSinkImpl();
        ImmutableFilterOutcome filterOutcome = new ImmutableFilterOutcome(new ImmutableFilterJob("testFilterJobName", null, null, null), ValidationCategory.VALID);
        outcomeSink.add(filterOutcome);
        outcomeSinks.add(outcomeSink);
         
        ConsumeRowResult consumeRowResult = new ConsumeRowResult(rows, outcomeSinks);

        AnalyzerBeanDescriptor<ValueDistributionAnalyzer> valueDistributionDescriptor = Descriptors.ofAnalyzer(ValueDistributionAnalyzer.class);
        BeanConfiguration beanConfiguration = new ImmutableBeanConfiguration(null);
        List<AnalyzerJob> analyzerJobs = new ArrayList<AnalyzerJob>();
        AnalyzerJob analyzerJobValid = new ImmutableAnalyzerJob("testAnalyzerValid", valueDistributionDescriptor, beanConfiguration, filterOutcome);
        AnalyzerJob analyzerJobInvalid = new ImmutableAnalyzerJob("testAnalyzerInvalid", valueDistributionDescriptor, beanConfiguration, new ImmutableFilterOutcome(null, ValidationCategory.INVALID));
        analyzerJobs.add(analyzerJobValid);
        analyzerJobs.add(analyzerJobInvalid);
        
        mapperEmitter.emit(consumeRowResult, analyzerJobs);
        
        Assert.assertEquals(1, emitList.size());
    }
    
    @After
    public void tearDown() {
        emitList.clear();
    }
}
