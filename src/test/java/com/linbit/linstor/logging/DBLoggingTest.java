package com.linbit.linstor.logging;

import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.objects.Node;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DBLoggingTest
{
    private static final String TEST_LOG_DIR = "build/test-logs";

    private void deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
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

        List<ErrorReport> reports = errReporter.listReports(false, null, null, Collections.singleton(errId));
        Assert.assertEquals(1, reports.size());

        errReporter.reportError(new RuntimeException("myruntimeexc"));
        reports = errReporter.listReports(false, null, null, Collections.emptySet());
        Assert.assertEquals(2, reports.size());
        Assert.assertEquals("myruntimeexc", reports.get(1).getExceptionMessage().orElse(""));
        Assert.assertTrue(reports.get(1).getOriginFile().isPresent());
        Assert.assertTrue(reports.get(1).getOriginMethod().isPresent());
        Assert.assertEquals("testDBLogging", reports.get(1).getOriginMethod().orElse(""));
        Assert.assertTrue(reports.get(1).getOriginLine().isPresent());
        Assert.assertEquals(Node.Type.CONTROLLER.name(), reports.get(1).getModuleString());

        errReporter.shutdown();
    }
}
