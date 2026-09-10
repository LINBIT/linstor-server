package com.linbit.linstor.logging;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.dbdrivers.H2FormatUtils;
import com.linbit.linstor.netcom.Peer;
import com.linbit.utils.TimeUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.dbcp2.BasicDataSource;

public class H2ErrorReporter
{
    private static final String DB_CRT_VERSION_TABLE = "CREATE TABLE IF NOT EXISTS VERSION (" +
        "VERSION_NUMBER INT);";
    private static final String DB_CRT_ERRORS_TABLE = """
        CREATE TABLE IF NOT EXISTS ERRORS (
          INSTANCE_EPOCH BIGINT,
          ERROR_NR INT,
          NODE VARCHAR(255),
          MODULE INT,
          ERROR_ID VARCHAR(32),
          DATETIME TIMESTAMP NOT NULL,
          VERSION VARCHAR(32),
          PEER VARCHAR(128),
          EXCEPTION VARCHAR(128),
          EXCEPTION_MESSAGE VARCHAR(2048),
          ORIGIN_FILE VARCHAR(128),
          ORIGIN_METHOD VARCHAR(128),
          ORIGIN_LINE INT,
          TEXT TEXT,
          PRIMARY KEY(INSTANCE_EPOCH, ERROR_NR, NODE));""";

    private final ErrorReporter errorReporter;
    private final BasicDataSource dataSource = new BasicDataSource();

    H2ErrorReporter(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
        dataSource.setUrl("jdbc:h2:" + errorReporter.getLogDirectory().toAbsolutePath() +
            "/error-report;COMPRESS=TRUE");

        dataSource.setMinIdle(5);
        dataSource.setMaxIdle(10);
        dataSource.setMaxOpenPreparedStatements(100);

        rotateLegacyErrorDB();
        setupErrorDB();
    }

    /**
     * H2 2.x cannot open database files written by H2 1.x. Since error-reports are expendable,
     * simply move a legacy database aside and start with a fresh one.
     */
    private void rotateLegacyErrorDB()
    {
        Path errorDb = errorReporter.getLogDirectory().toAbsolutePath().resolve("error-report.mv.db");
        try
        {
            if (Files.isRegularFile(errorDb) &&
                H2FormatUtils.loadedH2MajorVersion() >= 2 &&
                H2FormatUtils.isLegacyMvDbFormat(errorDb))
            {
                Path backup = errorDb.resolveSibling(
                    "error-report.mv.db.h2v1-" +
                        TimeUtils.DTF_NO_SPACE.format(LocalDateTime.now(ZoneId.systemDefault())) + ".bak");
                Files.move(errorDb, backup);
                errorReporter.logInfo(
                    "Moved error-report database written by H2 1.x to %s, starting with a fresh database",
                    backup
                );
            }
        }
        catch (IOException ioExc)
        {
            errorReporter.logError("Unable to rotate legacy error-reports database %s: %s", errorDb, ioExc);
        }
    }

    private void setupErrorDB()
    {
        try
        (
            Connection con = dataSource.getConnection();
            Statement stmt = con.createStatement();
        )
        {
            stmt.executeUpdate(DB_CRT_VERSION_TABLE);

            try (ResultSet rs = stmt.executeQuery("SELECT VERSION_NUMBER FROM VERSION"))
            {
                if (rs.next())
                {
                    int versionNumber = rs.getInt("VERSION_NUMBER");
                    errorReporter.logInfo("ErrorReporter DB version %d found.", versionNumber);
                    // upgrade db?
                }
                else
                {
                    // db empty
                    stmt.executeUpdate(DB_CRT_ERRORS_TABLE);
                    stmt.executeUpdate("CREATE INDEX IF NOT EXISTS IDX_ERRORS_DT ON ERRORS (DATETIME)");
                    stmt.executeUpdate("INSERT INTO VERSION (VERSION_NUMBER) VALUES (1)");
                    errorReporter.logInfo("ErrorReporter DB first time init.");
                }
            }
        }
        catch (SQLException sqlExc)
        {
            errorReporter.logError("Unable to operate the error-reports database: " + sqlExc);
        }
    }

    public void writeErrorReportToDB(
        long reportNr,
        @Nullable Peer client,
        Throwable errorInfo,
        long instanceEpoch,
        LocalDateTime errorTime,
        String nodeName,
        String module,
        byte[] errorReportText)
    {
        StackTraceElement[] traceItems = errorInfo.getStackTrace();
        @Nullable String originFile = traceItems.length > 0 ? traceItems[0].getFileName() : null;
        @Nullable String originMethod = traceItems.length > 0 ? traceItems[0].getMethodName() : null;
        @Nullable Integer originLine = traceItems.length > 0 ? traceItems[0].getLineNumber() : null;
        String excMsg = errorInfo.getMessage();

        try
        (
            Connection con = dataSource.getConnection();
            PreparedStatement stmt = con.prepareStatement("INSERT INTO ERRORS" +
                 " (INSTANCE_EPOCH, ERROR_NR, NODE, MODULE, ERROR_ID, DATETIME, VERSION, PEER," +
                 " EXCEPTION, EXCEPTION_MESSAGE, ORIGIN_FILE, ORIGIN_METHOD, ORIGIN_LINE, TEXT)" +
                 " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        )
        {
            int fieldIdx = 1;
            stmt.setLong(fieldIdx++, instanceEpoch);
            stmt.setLong(fieldIdx++, reportNr);
            stmt.setString(fieldIdx++, nodeName);
            stmt.setInt(fieldIdx++, (int) (module.equalsIgnoreCase(LinStor.CONTROLLER_MODULE) ?
                Node.Type.CONTROLLER.getFlagValue() : Node.Type.SATELLITE.getFlagValue()));
            stmt.setString(fieldIdx++, String.format("%s-%06d", errorReporter.getInstanceId(), reportNr));
            // errorTime is the UTC wall-clock of the report header; converting it with the system zone
            // (TimeUtils.getEpochMillis) shifts the stored instant by the host's UTC offset
            stmt.setTimestamp(fieldIdx++, new Timestamp(errorTime.toInstant(ZoneOffset.UTC).toEpochMilli()));
            stmt.setString(fieldIdx++, LinStor.VERSION_INFO_PROVIDER.getVersion());
            stmt.setString(fieldIdx++, client != null ? client.toString() : null);
            stmt.setString(fieldIdx++, errorInfo.getClass().getSimpleName());
            stmt.setString(fieldIdx++, excMsg != null ? excMsg.substring(0, Math.min(excMsg.length(), 2048)) : null);
            stmt.setString(fieldIdx++, originFile);
            stmt.setString(fieldIdx++, originMethod);
            if (originLine != null)
            {
                stmt.setInt(fieldIdx++, originLine);
            }
            else
            {
                stmt.setNull(fieldIdx++, Types.INTEGER);
            }
            stmt.setClob(fieldIdx, new InputStreamReader(new ByteArrayInputStream(errorReportText), StandardCharsets.UTF_8));

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            errorReporter.logError("Unable to write error report to DB: " + sqlExc.getMessage());
        }
    }

    public ErrorReportResult listReports(
        boolean withText,
        @Nullable final Instant since,
        @Nullable final Instant to,
        final Set<String> ids,
        @Nullable final Long limit,
        @Nullable final Long offset
    )
    {
        return listReports(withText, since, to, ids, limit, offset, null, null);
    }

    public ErrorReportResult listReports(
        boolean withText,
        @Nullable final Instant since,
        @Nullable final Instant to,
        final Set<String> ids,
        @Nullable final Long limit,
        @Nullable final Long offset,
        @Nullable final ErrorReportSortBy sortBy,
        @Nullable final Boolean sortAsc
    )
    {
        long count = 0;
        ArrayList<ErrorReport> errors = new ArrayList<>();
        StringBuilder where = new StringBuilder("1=1");
        ArrayList<Object> filterParams = new ArrayList<>();

        if (!ids.isEmpty())
        {
            where.append(" AND ERROR_ID IN (");
            for (String id : ids)
            {
                where.append("?,");
                filterParams.add(id);
            }
            where.setCharAt(where.length() - 1, ')');
        }

        if (since != null)
        {
            where.append(" AND DATETIME >= ?");
            filterParams.add(new Timestamp(since.toEpochMilli()));
        }

        if (to != null)
        {
            where.append(" AND DATETIME <= ?");
            filterParams.add(new Timestamp(to.toEpochMilli()));
        }

        // the sort column is whitelisted through the ErrorReportSortBy enum, only filter values are
        // bound as statement parameters. DATETIME/ERROR_ID keep the ordering stable so that paging
        // neither duplicates nor skips reports
        final ErrorReportSortBy sortField = sortBy != null ? sortBy : ErrorReportSortBy.ERROR_TIME;
        final boolean ascending = sortAsc != null && sortAsc;
        final String orderByStr = " ORDER BY " + sortField.getColumnName() +
            (ascending ? " ASC NULLS FIRST" : " DESC NULLS LAST") +
            ", DATETIME DESC, ERROR_ID";

        final String columnsStr = "INSTANCE_EPOCH, ERROR_NR, NODE, MODULE, ERROR_ID, DATETIME, VERSION, PEER," +
            " EXCEPTION, EXCEPTION_MESSAGE, ORIGIN_FILE, ORIGIN_METHOD, ORIGIN_LINE" + (withText ? ", TEXT" : "");
        final String countStmtStr = "SELECT COUNT(*) FROM ERRORS WHERE " + where;
        String selectStmtStr = "SELECT " +
            columnsStr +
            " FROM ERRORS" +
            " WHERE " + where +
            orderByStr;
        final long offsetRows = offset != null && offset > 0 ? offset : 0L;
        if (limit != null)
        {
            selectStmtStr += " OFFSET " + offsetRows + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
        }
        else if (offsetRows > 0)
        {
            selectStmtStr += " OFFSET " + offsetRows + " ROWS";
        }
        try
        (
            Connection con = dataSource.getConnection();
            PreparedStatement countStmt = con.prepareStatement(countStmtStr);
            PreparedStatement selectStmt = con.prepareStatement(selectStmtStr)
        )
        {
            for (int paramIdx = 0; paramIdx < filterParams.size(); paramIdx++)
            {
                countStmt.setObject(paramIdx + 1, filterParams.get(paramIdx));
                selectStmt.setObject(paramIdx + 1, filterParams.get(paramIdx));
            }

            try (ResultSet countResult = countStmt.executeQuery())
            {
                countResult.next();
                count = countResult.getLong(1);
            }

            try (ResultSet rslt = selectStmt.executeQuery())
            {
                while (rslt.next())
                {
                    @Nullable String text = null;
                    if (withText)
                    {
                        Clob clob = rslt.getClob("TEXT");
                        // this is how you get the whole string back from a CLOB
                        text = clob.getSubString(1, (int) clob.length());
                    }
                    @Nullable String nodeName = rslt.getString("NODE");
                    if (nodeName == null)
                    {
                        throw new ImplementationError("nodeName must not be null");
                    }
                    errors.add(
                        new ErrorReport(
                            nodeName,
                            Node.Type.getByValue(rslt.getInt("MODULE")),
                            "ErrorReport-" + rslt.getString("ERROR_ID") + ".log",
                            rslt.getString("VERSION"),
                            rslt.getString("PEER"),
                            rslt.getString("EXCEPTION"),
                            rslt.getString("EXCEPTION_MESSAGE"),
                            rslt.getString("ORIGIN_FILE"),
                            rslt.getString("ORIGIN_METHOD"),
                            rslt.getInt("ORIGIN_LINE"),
                            Instant.ofEpochMilli(rslt.getTimestamp("DATETIME").getTime()),
                            text
                        )
                    );
                }
            }
        }
        catch (SQLException sqlExc)
        {
            errorReporter.logError("Unable to operate on error-reports database: " + sqlExc.getMessage());
        }

        return new ErrorReportResult(count, errors);
    }

    public List<String> deleteErrorReports(
        @Nullable final Instant since,
        @Nullable final Instant to,
        @Nullable final String exception,
        @Nullable final String version,
        @Nullable final List<String> ids)
    {
        // prevent an "empty" where clause(delete all)
        if (since == null && to == null && exception == null && version == null && (ids == null || ids.isEmpty()))
        {
            return Collections.emptyList();
        }

        try
        {
            StringBuilder delStmt = new StringBuilder();
            String delPrefix = "DELETE FROM ERRORS WHERE 1=1";
            delStmt.append(delPrefix);
            if (to != null)
            {
                delStmt.append(" AND DATETIME < ?");
            }
            if (since != null)
            {
                delStmt.append(" AND DATETIME >= ?");
            }
            if (exception != null)
            {
                delStmt.append(" AND EXCEPTION=?");
            }
            if (version != null)
            {
                delStmt.append(" AND VERSION=?");
            }
            if (ids != null && !ids.isEmpty())
            {
                delStmt.append(" AND ERROR_ID in (");

                for (String ignored : ids)
                {
                    delStmt.append("?,");
                }

                delStmt.deleteCharAt(delStmt.length() - 1);
                delStmt.append(")");
            }

            String selStmt = "SELECT ERROR_ID FROM ERRORS WHERE 1=1" + delStmt.substring(delPrefix.length());

            try
            (
                Connection con = dataSource.getConnection();
                PreparedStatement pSelStmt = con.prepareStatement(selStmt);
                PreparedStatement pDelStmt = con.prepareStatement(delStmt.toString())
            )
            {
                int index = 1;
                if (to != null)
                {
                    pSelStmt.setTimestamp(index, new java.sql.Timestamp(to.toEpochMilli()));
                    pDelStmt.setTimestamp(index++, new java.sql.Timestamp(to.toEpochMilli()));
                }
                if (since != null)
                {
                    pSelStmt.setTimestamp(index, new java.sql.Timestamp(since.toEpochMilli()));
                    pDelStmt.setTimestamp(index++, new java.sql.Timestamp(since.toEpochMilli()));
                }
                if (exception != null)
                {
                    pSelStmt.setString(index, exception);
                    pDelStmt.setString(index++, exception);
                }
                if (version != null)
                {
                    pSelStmt.setString(index, version);
                    pDelStmt.setString(index++, version);
                }
                if (ids != null)
                {
                    for (String id : ids)
                    {
                        pSelStmt.setString(index, id);
                        pDelStmt.setString(index++, id);
                    }
                }

                var delIds = new ArrayList<String>();
                con.setAutoCommit(false);
                try (ResultSet rs = pSelStmt.executeQuery())
                {
                    while (rs.next())
                    {
                        delIds.add(rs.getString("ERROR_ID"));
                    }
                }
                pDelStmt.executeUpdate();

                con.commit();
                return Collections.unmodifiableList(delIds);
            }
        }
        catch (SQLException sqlExc)
        {
            final String errorMsg = "Unable to operate on error-reports database: " + sqlExc.getMessage();
            final @Nullable String errorId = errorReporter.reportError(sqlExc);
            ApiCallRcImpl.ApiCallRcEntry entry = ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_SQL, errorMsg);
            if (errorId != null)
            {
                entry.addErrorId(errorId);
            }

            throw new ApiRcException(entry);
        }
    }

    public void shutdown() throws SQLException
    {
        dataSource.close();
    }
}
