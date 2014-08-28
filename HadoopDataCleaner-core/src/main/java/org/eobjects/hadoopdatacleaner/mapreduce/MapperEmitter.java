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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.job.AnalyzerJob;
import org.eobjects.analyzer.job.Outcome;
import org.eobjects.analyzer.job.runner.ConsumeRowResult;
import org.eobjects.analyzer.job.runner.OutcomeSink;
import org.eobjects.analyzer.util.LabelUtils;
import org.eobjects.hadoopdatacleaner.datastores.RowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperEmitter {
    
    private static final Logger logger = LoggerFactory.getLogger(MapperEmitter.class);

    public static interface Callback {
        public void write(Text text, SortedMapWritable map) throws IOException, InterruptedException;
    }

    private final Callback callback;

    /**
     * 
     */
    public MapperEmitter(Callback callback) {
        this.callback = callback;
    }
    
    public void emit(ConsumeRowResult consumeRowResult, List<AnalyzerJob> analyzerJobs) throws IOException, InterruptedException {
        Iterator<OutcomeSink> outcomeSinksIterator = consumeRowResult.getOutcomeSinks().iterator();
        for (InputRow transformedRow : consumeRowResult.getRows()) {
            SortedMapWritable rowWritable = RowUtils.inputRowToSortedMapWritable(transformedRow);
//            logger.debug("The row after being converted to SortedMapWritable: ");
//            RowUtils.printSortedMapWritable(rowWritable, logger);
            // FIXME: hasNext() somehow !
//            logger.info("");
            OutcomeSink outcomeSink = outcomeSinksIterator.next();
            for (AnalyzerJob analyzerJob : analyzerJobs) {
                if (isAnalyzerSatisfied(analyzerJob, outcomeSink)) {
                    String analyzerLabel = LabelUtils.getLabel(analyzerJob);
//                    logger.info("Emitting " + transformedRow + " to " + analyzerLabel);
                    callback.write(new Text(analyzerLabel), rowWritable);
                }
            }
        }
    }
    
    private boolean isAnalyzerSatisfied(AnalyzerJob analyzerJob, OutcomeSink outcomeSink) {
        for (Outcome requirement : analyzerJob.getRequirements()) {
            if (!outcomeSink.contains(requirement)) {
                return false;
            }
        }
        return true;
    }

}
