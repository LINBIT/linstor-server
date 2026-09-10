package com.linbit.linstor.logging;

import com.linbit.linstor.annotation.Nullable;

import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;

/**
 * Sort fields for error-report listing. Each value maps to the whitelisted column of the local
 * error-report database used for node-local sorting, and to a comparator with the same ordering
 * used when merging the results of multiple nodes.
 */
public enum ErrorReportSortBy
{
    ERROR_TIME("error_time", "DATETIME"),
    NODE_NAME("node_name", "NODE"),
    MODULE("module", "MODULE"),
    EXCEPTION("exception", "EXCEPTION"),
    EXCEPTION_MESSAGE("exception_message", "EXCEPTION_MESSAGE"),
    ORIGIN_FILE("origin_file", "ORIGIN_FILE"),
    FILENAME("filename", "ERROR_ID"),
    VERSION("version", "VERSION"),
    PEER("peer", "PEER");

    private final String apiValue;
    private final String columnName;

    ErrorReportSortBy(String apiValueRef, String columnNameRef)
    {
        apiValue = apiValueRef;
        columnName = columnNameRef;
    }

    /**
     * The value of this sort field in the REST API and in controller-satellite messages.
     */
    public String getApiValue()
    {
        return apiValue;
    }

    /**
     * The whitelisted column of the ERRORS table this sort field maps to.
     */
    public String getColumnName()
    {
        return columnName;
    }

    /**
     * Ascending comparator over the sort field, sorting absent values first - the same ordering
     * as the "ASC NULLS FIRST" used in the database statement. Reversing it therefore also matches
     * "DESC NULLS LAST".
     */
    public Comparator<ErrorReport> getComparator()
    {
        return switch (this)
        {
            case ERROR_TIME -> Comparator.comparing(ErrorReport::getDateTime);
            case NODE_NAME -> Comparator.comparing(ErrorReport::getNodeName);
            case MODULE -> Comparator.comparingLong(rpt -> rpt.getModule().getFlagValue());
            case EXCEPTION -> comparingOptional(ErrorReport::getException);
            case EXCEPTION_MESSAGE -> comparingOptional(ErrorReport::getExceptionMessage);
            case ORIGIN_FILE -> comparingOptional(ErrorReport::getOriginFile);
            case FILENAME -> Comparator.comparing(ErrorReport::getFileName);
            case VERSION -> comparingOptional(ErrorReport::getVersion);
            case PEER -> comparingOptional(ErrorReport::getPeer);
        };
    }

    /**
     * Parses the given api value (case-insensitive) into its sort field.
     *
     * @return the matching sort field, or null if no field matches
     */
    public static @Nullable ErrorReportSortBy parse(@Nullable String value)
    {
        @Nullable ErrorReportSortBy ret = null;
        if (value != null)
        {
            for (ErrorReportSortBy sortBy : values())
            {
                if (sortBy.apiValue.equalsIgnoreCase(value))
                {
                    ret = sortBy;
                    break;
                }
            }
        }
        return ret;
    }

    private static Comparator<ErrorReport> comparingOptional(Function<ErrorReport, Optional<String>> getter)
    {
        return Comparator.comparing(
            rpt -> getter.apply(rpt).orElse(null),
            Comparator.nullsFirst(Comparator.naturalOrder())
        );
    }
}
