package com.linbit.linstor.logging;

import com.linbit.utils.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class ErrorReportResult
{
    private long totalCount;
    private final ArrayList<ErrorReport> errorReports = new ArrayList<>();
    /**
     * The key of this map is a Pair<NodeName,Module>.
     */
    private final HashMap<Pair<String, String>, Long> nodeCounts = new HashMap<>();

    public ErrorReportResult(long totalCountRef, Collection<ErrorReport> errorReportsRef)
    {
        totalCount = totalCountRef;
        errorReports.addAll(errorReportsRef);
    }

    /**
     * Add another ErrorReportResult to this, but with nodeName and module info.
     * @param nodeName Name of the satellite node this result comes from.
     * @param module Module name of the node
     * @param other ErrorReportResult to add.
     * @return This ErrorReportResult instance
     */
    public ErrorReportResult addErrorReportResult(
        String nodeName,
        String module,
        ErrorReportResult other
    )
    {
        totalCount += other.totalCount;
        nodeCounts.put(new Pair<>(nodeName, module), other.totalCount);
        errorReports.addAll(other.errorReports);
        return this;
    }

    /**
     * Inplace sorts the error reports by the LinstorFile::compareTo method.
     * @return This ErrorReportResult instance
     */
    public ErrorReportResult sort()
    {
        errorReports.sort(LinstorFile::compareTo);
        return this;
    }

    /**
     * Total count of error reports with applied filters.
     * @return count of error reports with filters applied, but not respecting limit/offset.
     */
    public long getTotalCount()
    {
        return totalCount;
    }

    public HashMap<Pair<String, String>, Long> getNodeCounts()
    {
        return nodeCounts;
    }

    /**
     * List of ErrorReport data.
     * @return ErrorReport list.
     */
    public List<ErrorReport> getErrorReports()
    {
        return errorReports;
    }
}
