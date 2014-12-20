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
package org.eobjects.hadoopdatacleaner;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ToolRunner;
import org.eobjects.hadoopdatacleaner.tools.FlatFileTool;
import org.junit.Assert;
import org.junit.Test;

public class FlatFileToolIntegrationTest {

    FlatFileTool flatFileTool;

    @Test
    public void test() throws Exception {
        String[] args = new String[3];
        args[0] = "src/test/resources/simplest_countrycodes_job.analysis.xml";
        args[1] = "src/test/resources/countrycodes.csv";
        args[2] = "output";
        String analysisJobXml = FileUtils.readFileToString(new File(args[0]));
        flatFileTool = new FlatFileTool(analysisJobXml, "countrycodes.csv", "output");
        int exitCode = ToolRunner.run(flatFileTool, args);
        Assert.assertEquals("The exit code of the FlatFileTool should be 0.", 0, exitCode);
    }

}
