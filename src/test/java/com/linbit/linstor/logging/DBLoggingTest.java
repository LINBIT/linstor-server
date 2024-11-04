package com.linbit.linstor.logging;

import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.objects.Node;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DBLoggingTest
{
    private static final String TEST_LOG_DIR = "build/test-logs";

    // cleanup-method, doesn't matter if the dir existed and was deleted or didn't exist in the first place
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private void deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null)
        {
            for (File file : allContents)
            {
                deleteDirectory(file);
            }
        }
        directoryToBeDeleted.delete();

        Assert.assertFalse("Directory still exists", Files.exists(directoryToBeDeleted.toPath()));
    }

    @Before
    public void setUp()
    {
        deleteDirectory(Paths.get(TEST_LOG_DIR).toFile());
    }

    @After
    public void tearDown()
    {
    }

    @Test
    public void testDBLogging() throws Exception
    {
        StdErrorReporter errReporter = new StdErrorReporter(
            LinStor.CONTROLLER_MODULE,
            Paths.get(TEST_LOG_DIR),
            false,
            "testnode",
            "TRACE",
            "TRACE",
            () -> null
        );
        String errId = errReporter.reportError(new NullPointerException());

        ErrorReportResult reports = errReporter.listReports(false, null, null, Collections.singleton(errId), null, null);
        Assert.assertEquals(1, reports.getTotalCount());

        errReporter.reportError(new RuntimeException("myruntimeexc"));
        reports = errReporter.listReports(false, null, null, Collections.emptySet(), null, null);
        reports.sort();
        Assert.assertEquals(2, reports.getTotalCount());
        Assert.assertEquals("myruntimeexc", reports.getErrorReports().get(1).getExceptionMessage().orElse(""));
        Assert.assertTrue(reports.getErrorReports().get(1).getOriginFile().isPresent());
        Assert.assertTrue(reports.getErrorReports().get(1).getOriginMethod().isPresent());
        Assert.assertEquals("testDBLogging", reports.getErrorReports().get(1).getOriginMethod().orElse(""));
        Assert.assertTrue(reports.getErrorReports().get(1).getOriginLine().isPresent());
        Assert.assertEquals(Node.Type.CONTROLLER.name(), reports.getErrorReports().get(1).getModuleString());

        errReporter.shutdown();
    }
}
