package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.logging.ErrorReportResult;
import com.linbit.linstor.logging.ErrorReportSortBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class CtrlErrorListPagingTest
{
    private static ErrorReport report(String nodeName, String fileName, Instant time, String exception)
    {
        return new ErrorReport(
            nodeName,
            Node.Type.SATELLITE,
            fileName,
            "1.0.0",
            null,
            exception,
            null,
            null,
            null,
            null,
            time,
            null
        );
    }

    private static List<String> fileNames(ErrorReportResult result)
    {
        List<String> ret = new ArrayList<>();
        for (ErrorReport rpt : result.getErrorReports())
        {
            ret.add(rpt.getFileName());
        }
        return ret;
    }

    @Test
    public void testMergeSortSlice()
    {
        Instant base = Instant.ofEpochMilli(1_000_000);

        ErrorReportResult merged = new ErrorReportResult(0, Collections.emptyList());
        // each node only returns its first offset+limit reports, but reports its full count
        merged.addErrorReportResult(
            "nodeA",
            Node.Type.SATELLITE.name(),
            new ErrorReportResult(
                30,
                List.of(
                    report("nodeA", "ErrorReport-A-000001.log", base.plusSeconds(30), "NullPointerException"),
                    report("nodeA", "ErrorReport-A-000002.log", base.plusSeconds(10), "RuntimeException")
                )
            )
        );
        merged.addErrorReportResult(
            "nodeB",
            Node.Type.SATELLITE.name(),
            new ErrorReportResult(
                25,
                List.of(
                    report("nodeB", "ErrorReport-B-000001.log", base.plusSeconds(20), "IllegalStateException"),
                    report("nodeB", "ErrorReport-B-000002.log", base.plusSeconds(40), "TimeoutException")
                )
            )
        );

        Assert.assertEquals(55, merged.getTotalCount());

        // newest first, skip the newest, keep the following two
        merged
            .sort(CtrlErrorListApiCallHandler.pageComparator(ErrorReportSortBy.ERROR_TIME, false))
            .slice(1, 2);

        Assert.assertEquals(
            List.of("ErrorReport-A-000001.log", "ErrorReport-B-000001.log"),
            fileNames(merged)
        );
        // slicing must not change the total count
        Assert.assertEquals(55, merged.getTotalCount());
    }

    @Test
    public void testTiebreakersKeepPagingStable()
    {
        Instant time = Instant.ofEpochMilli(1_000_000);

        ErrorReportResult merged = new ErrorReportResult(0, Collections.emptyList());
        merged.addErrorReportResult(
            "nodeB",
            Node.Type.SATELLITE.name(),
            new ErrorReportResult(
                2,
                List.of(
                    report("nodeB", "ErrorReport-B-000002.log", time, "SameException"),
                    report("nodeB", "ErrorReport-B-000001.log", time, "SameException")
                )
            )
        );
        merged.addErrorReportResult(
            "nodeA",
            Node.Type.SATELLITE.name(),
            new ErrorReportResult(
                1,
                List.of(report("nodeA", "ErrorReport-A-000001.log", time, "SameException"))
            )
        );

        // all sort fields equal: node name and filename break the tie deterministically
        merged.sort(CtrlErrorListApiCallHandler.pageComparator(ErrorReportSortBy.EXCEPTION, true));

        Assert.assertEquals(
            List.of("ErrorReport-A-000001.log", "ErrorReport-B-000001.log", "ErrorReport-B-000002.log"),
            fileNames(merged)
        );
    }

    @Test
    public void testSortAscendingWithAbsentValuesFirst()
    {
        Instant base = Instant.ofEpochMilli(1_000_000);

        ErrorReportResult merged = new ErrorReportResult(0, Collections.emptyList());
        merged.addErrorReportResult(
            "nodeA",
            Node.Type.SATELLITE.name(),
            new ErrorReportResult(
                2,
                List.of(
                    report("nodeA", "ErrorReport-A-000001.log", base, "RuntimeException"),
                    new ErrorReport(
                        "nodeA",
                        Node.Type.SATELLITE,
                        "ErrorReport-A-000002.log",
                        "1.0.0",
                        null,
                        null, // no exception recorded
                        null,
                        null,
                        null,
                        null,
                        base.plusSeconds(5),
                        null
                    )
                )
            )
        );

        merged.sort(CtrlErrorListApiCallHandler.pageComparator(ErrorReportSortBy.EXCEPTION, true));
        Assert.assertEquals(
            List.of("ErrorReport-A-000002.log", "ErrorReport-A-000001.log"),
            fileNames(merged)
        );
    }
}
