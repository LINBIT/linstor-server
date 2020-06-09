package com.linbit.linstor.logging;

import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.netcom.Peer;

import javax.annotation.Nullable;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.commons.dbcp2.BasicDataSource;

public class H2ErrorReporter {
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
        dataSource.setUrl("jdbc:h2:" + errorReporter.getLogDirectory().toAbsolutePath().toString() +
            "/error-report;COMPRESS=TRUE");

        dataSource.setMinIdle(5);
        dataSource.setMaxIdle(10);
        dataSource.setMaxOpenPreparedStatements(100);

        setupErrorDB();
    }

    private void setupErrorDB()
    {
        try (Connection con = dataSource.getConnection();
             Statement stmt = con.createStatement()) {
            stmt.executeUpdate(DB_CRT_VERSION_TABLE);

            try (ResultSet rs = stmt.executeQuery("SELECT VERSION_NUMBER FROM VERSION")) {
                if (rs.next()) {
                    int versionNumber = rs.getInt("VERSION_NUMBER");
                    errorReporter.logInfo("ErrorReporter DB version %d found.", versionNumber);
                    // upgrade db?
                } else {
                    // db empty
                    stmt.executeUpdate(DB_CRT_ERRORS_TABLE);
                    stmt.executeUpdate("CREATE INDEX IF NOT EXISTS IDX_ERRORS_DT ON ERRORS (DATETIME)");
                    stmt.executeUpdate("SET COMPRESS_LOB LZF");
                    stmt.executeUpdate("INSERT INTO VERSION (VERSION_NUMBER) VALUES (1)");
                    errorReporter.logInfo("ErrorReporter DB first time init.");
                }
            }
        } catch(SQLException sqlExc) {
            errorReporter.logError("Unable to operate the error-reports database: " + sqlExc);
        }
    }

    public void writeErrorReportToDB(
        long reportNr,
        Peer client,
        Throwable errorInfo,
        long instanceEpoch,
        Date errorTime,
        String nodeName,
        String module,
        byte[] errorReportText)
    {
        StackTraceElement[] traceItems = errorInfo.getStackTrace();
        String originFile = traceItems.length > 0 ? traceItems[0].getFileName() : null;
        String originMethod = traceItems.length > 0 ? traceItems[0].getMethodName() : null;
        Integer originLine = traceItems.length > 0 ? traceItems[0].getLineNumber() : null;
        String excMsg = errorInfo.getMessage();

        try (Connection con = dataSource.getConnection();
             PreparedStatement stmt = con.prepareStatement("INSERT INTO ERRORS" +
                 " (INSTANCE_EPOCH, ERROR_NR, NODE, MODULE, ERROR_ID, DATETIME, VERSION, PEER," +
                 " EXCEPTION, EXCEPTION_MESSAGE, ORIGIN_FILE, ORIGIN_METHOD, ORIGIN_LINE, TEXT)" +
                 " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            int fieldIdx = 1;
            stmt.setLong(fieldIdx++, instanceEpoch);
            stmt.setLong(fieldIdx++, reportNr);
            stmt.setString(fieldIdx++, nodeName);
            stmt.setInt(fieldIdx++, (int)(module.equalsIgnoreCase(LinStor.CONTROLLER_MODULE) ?
                Node.Type.CONTROLLER.getFlagValue() : Node.Type.SATELLITE.getFlagValue()));
            stmt.setString(fieldIdx++, String.format("%s-%06d", errorReporter.getInstanceId(), reportNr));
            stmt.setTimestamp(fieldIdx++, new Timestamp(errorTime.getTime()));
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
            else {
                stmt.setNull(fieldIdx++, Types.INTEGER);
            }
            stmt.setClob(fieldIdx, new InputStreamReader(new ByteArrayInputStream(errorReportText)));

            stmt.executeUpdate();
        } catch (SQLException sqlExc) {
            errorReporter.logError("Unable to write error report to DB: " + sqlExc.getMessage());
        }
    }

    public List<ErrorReport> listReports(
        boolean withText,
        @Nullable final Date since,
        @Nullable final Date to,
        final Set<String> ids
    )
    {
        ArrayList<ErrorReport> errors = new ArrayList<>();
        String where = "1=1";

        if (!ids.isEmpty()) {
            where += " AND ERROR_ID IN ('" + String.join("','", ids) + "')";
        }

        if (since != null)
        {
            where += " AND DATETIME >= '" + since.toString() + "'";
        }

        if (to != null)
        {
            where += " AND DATETIME <= '" + to.toString() + "'";
        }

        final String stmtStr = "SELECT" +
            " INSTANCE_EPOCH, ERROR_NR, NODE, MODULE, ERROR_ID, DATETIME, VERSION, PEER," +
            " EXCEPTION, EXCEPTION_MESSAGE, ORIGIN_FILE, ORIGIN_METHOD, ORIGIN_LINE, TEXT" +
            " FROM ERRORS" +
            " WHERE " + where +
            " ORDER BY DATETIME";
        try (Connection con = dataSource.getConnection();
             Statement stmt = con.createStatement();
             ResultSet rslt = stmt.executeQuery(stmtStr)) {
            while(rslt.next()) {
                String text = null;
                if (withText) {
                    Clob clob = rslt.getClob("TEXT");
                    // this is how you get the whole string back from a CLOB
                    text = clob.getSubString(1, (int)clob.length());
                }
                errors.add(new ErrorReport(
                    rslt.getString("NODE"),
                    Node.Type.getByValue(rslt.getInt("MODULE")),
                    "ErrorReport-" + rslt.getString("ERROR_ID") + ".log",
                    rslt.getString("VERSION"),
                    rslt.getString("PEER"),
                    rslt.getString("EXCEPTION"),
                    rslt.getString("EXCEPTION_MESSAGE"),
                    rslt.getString("ORIGIN_FILE"),
                    rslt.getString("ORIGIN_METHOD"),
                    rslt.getInt("ORIGIN_LINE"),
                    rslt.getTimestamp("DATETIME"),
                    text
                ));
            }
        } catch(SQLException sqlExc) {
            errorReporter.logError("Unable to operate on error-reports database: " + sqlExc.getMessage());
        }

        return errors;
    }
}
