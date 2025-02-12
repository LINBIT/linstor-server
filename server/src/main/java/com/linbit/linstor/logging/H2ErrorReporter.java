package com.linbit.linstor.logging;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.netcom.Peer;
import com.linbit.utils.TimeUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.commons.dbcp2.BasicDataSource;

public class H2ErrorReporter
{
    private static final String DB_CRT_VERSION_TABLE = "CREATE TABLE IF NOT EXISTS VERSION (" +
        "VERSION_NUMBER INT);";
    private static final String DB_CRT_ERRORS_TABLE = "CREATE TABLE IF NOT EXISTS ERRORS (\n" +
        "\tINSTANCE_EPOCH BIGINT,\n" +
        "\tERROR_NR INT,\n" +
        "\tNODE VARCHAR(255),\n" +
        "\tMODULE INT,\n" +
        "\tERROR_ID VARCHAR(32),\n" +
        "\tDATETIME TIMESTAMP NOT NULL,\n" +
        "\tVERSION VARCHAR(32),\n" +
        "\tPEER VARCHAR(128),\n" +
        "\tEXCEPTION VARCHAR(128),\n" +
        "\tEXCEPTION_MESSAGE VARCHAR(2048),\n" +
        "\tORIGIN_FILE VARCHAR(128),\n" +
        "\tORIGIN_METHOD VARCHAR(128),\n" +
        "\tORIGIN_LINE INT,\n" +
        "\tTEXT TEXT,\n" +
        "\tPRIMARY KEY(INSTANCE_EPOCH, ERROR_NR, NODE));";

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

        setupErrorDB();
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
                    stmt.executeUpdate("SET COMPRESS_LOB LZF");
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
        Peer client,
        Throwable errorInfo,
        long instanceEpoch,
        LocalDateTime errorTime,
        String nodeName,
        String module,
        byte[] errorReportText)
    {
        StackTraceElement[] traceItems = errorInfo.getStackTrace();
        String originFile = traceItems.length > 0 ? traceItems[0].getFileName() : null;
        String originMethod = traceItems.length > 0 ? traceItems[0].getMethodName() : null;
        Integer originLine = traceItems.length > 0 ? traceItems[0].getLineNumber() : null;
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
            stmt.setTimestamp(fieldIdx++, new Timestamp(TimeUtils.getEpochMillis(errorTime)));
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
            stmt.setClob(fieldIdx, new InputStreamReader(new ByteArrayInputStream(errorReportText)));

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            errorReporter.logError("Unable to write error report to DB: " + sqlExc.getMessage());
        }
    }

    public ErrorReportResult listReports(
        boolean withText,
        @Nullable final Date since,
        @Nullable final Date to,
        final Set<String> ids,
        @Nullable final Long limit,
        @Nullable final Long offset
    )
    {
        long count = 0;
        ArrayList<ErrorReport> errors = new ArrayList<>();
        String where = "1=1";

        if (!ids.isEmpty())
        {
            where += " AND ERROR_ID IN ('" + String.join("','", ids) + "')";
        }

        if (since != null)
        {
            where += " AND DATETIME >= '" + new java.sql.Date(since.getTime()) + "'";
        }

        if (to != null)
        {
            where += " AND DATETIME <= '" + new java.sql.Date(to.getTime()) + "'";
        }

        final String columnsStr = "INSTANCE_EPOCH, ERROR_NR, NODE, MODULE, ERROR_ID, DATETIME, VERSION, PEER," +
            " EXCEPTION, EXCEPTION_MESSAGE, ORIGIN_FILE, ORIGIN_METHOD, ORIGIN_LINE" + (withText ? ", TEXT" : "");
        final String countStmtStr = "SELECT COUNT(*) FROM ERRORS WHERE " + where;
        final String selectStmtStr = "SELECT " +
            columnsStr +
            " FROM ERRORS" +
            " WHERE " + where +
            " ORDER BY DATETIME DESC";
        // ignore offset for now
        final String stmtStr = limit != null ?
            selectStmtStr + " OFFSET 0 ROWS FETCH NEXT " + limit + " ROWS ONLY" :
            selectStmtStr;
        try
        (
            Connection con = dataSource.getConnection();
            Statement stmt = con.createStatement()
        )
        {
            ResultSet countResult = stmt.executeQuery(countStmtStr);
            countResult.next();
            count = countResult.getLong(1);
            countResult.close();

            ResultSet rslt = stmt.executeQuery(stmtStr);
            while (rslt.next())
            {
                String text = null;
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
                        new Date(rslt.getTimestamp("DATETIME").getTime()),
                        text
                    )
                );
            }
            rslt.close();
        }
        catch(SQLException sqlExc)
        {
            errorReporter.logError("Unable to operate on error-reports database: " + sqlExc.getMessage());
        }

        return new ErrorReportResult(count, errors);
    }

    public ApiCallRc deleteErrorReports(
        @Nullable final Date since,
        @Nullable final Date to,
        @Nullable final String exception,
        @Nullable final String version,
        @Nullable final List<String> ids)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        // prevent an "empty" where clause(delete all)
        if (since == null && to == null && exception == null && version == null && (ids == null || ids.isEmpty()))
        {
            return apiCallRc;
        }

        try
        {
            int index = 1;
            StringBuilder stmt = new StringBuilder();
            stmt.append("DELETE FROM ERRORS WHERE 1=1");
            if (to != null)
            {
                stmt.append(" AND DATETIME < ?");
            }
            if (since != null)
            {
                stmt.append(" AND DATETIME >= ?");
            }
            if (exception != null)
            {
                stmt.append(" AND EXCEPTION=?");
            }
            if (version != null)
            {
                stmt.append(" AND VERSION=?");
            }
            if (ids != null && !ids.isEmpty())
            {
                stmt.append(" AND ERROR_ID in (");

                for (String ignored : ids)
                {
                    stmt.append("?,");
                }

                stmt.deleteCharAt(stmt.length() - 1);
                stmt.append(")");
            }

            try
            (
                Connection con = dataSource.getConnection();
                PreparedStatement pStmt = con.prepareStatement(stmt.toString());
            )
            {
                if (to != null)
                {
                    pStmt.setTimestamp(index++, new java.sql.Timestamp(to.getTime()));
                }
                if (since != null)
                {
                    pStmt.setTimestamp(index++, new java.sql.Timestamp(since.getTime()));
                }
                if (exception != null)
                {
                    pStmt.setString(index++, exception);
                }
                if (version != null)
                {
                    pStmt.setString(index++, version);
                }
                if (ids != null)
                {
                    for (String id : ids)
                    {
                        pStmt.setString(index++, id);
                    }
                }

                final int deleted = pStmt.executeUpdate();
                if (deleted > 0)
                {
                    apiCallRc.addEntry(String.format("Deleted %d error-report(s)", deleted), ApiConsts.DELETED);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            final String errorMsg = "Unable to operate on error-reports database: " + sqlExc.getMessage();
            final String errorId = errorReporter.reportError(sqlExc);
            apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_SQL, errorMsg).addErrorId(errorId));
        }

        return apiCallRc;
    }

    public void shutdown() throws SQLException
    {
        dataSource.close();
    }
}
