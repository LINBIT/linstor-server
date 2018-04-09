package com.linbit.linstor.logging;

import com.linbit.AutoIndent;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.event.Level;

public class StderrErrorReporter extends BaseErrorReporter implements ErrorReporter
{
    private AtomicLong errorNr = new AtomicLong(0L);

    public StderrErrorReporter(String moduleName)
    {
        super(moduleName);
    }

    @Override
    public boolean isTraceEnabled()
    {
        return true;
    }

    @Override
    public void setTraceEnabled(AccessContext accCtx, boolean flag) throws AccessDeniedException
    {
    }

    @Override
    public void logTrace(String format, Object... args)
    {
        System.err.println("TRACE:   " + String.format(format, args));
    }

    @Override
    public void logDebug(String format, Object... args)
    {
        System.err.println("DEBUG:   " + String.format(format, args));
    }

    @Override
    public void logInfo(String format, Object... args)
    {
        System.err.println("INFO:    " + String.format(format, args));
    }

    @Override
    public void logWarning(String format, Object... args)
    {
        System.err.println("WARNING: " + String.format(format, args));
    }

    @Override
    public void logError(String format, Object... args)
    {
        System.err.println("ERROR:   " + String.format(format, args));
    }

    @Override
    public String getInstanceId()
    {
        return "AFFE00";
    }

    @Override
    public String reportError(Throwable errorInfo)
    {
        return reportError(Level.ERROR, errorInfo, null, null, null);
    }

    @Override
    public String reportError(Level logLevel, Throwable errorInfo)
    {
        return reportError(logLevel, errorInfo, null, null, null);
    }

    @Override
    public String reportError(
        Throwable errorInfo,
        AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        return reportErrorImpl(Level.ERROR, errorInfo, accCtx, client, contextInfo);
    }

    @Override
    public String reportError(
        Level logLevel,
        Throwable errorInfo,
        AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        return reportErrorImpl(logLevel, errorInfo, accCtx, client, contextInfo);
    }

    @Override
    public String reportProblem(
        Level logLevel,
        LinStorException errorInfo,
        AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        return reportProblemImpl(logLevel, errorInfo, accCtx, client, contextInfo);
    }

    private String reportErrorImpl(
        Level logLevel,
        Throwable errorInfoRef,
        AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        PrintStream output = System.err;
        String logName = null;
        try
        {
            long reportNr = errorNr.getAndIncrement();

            Throwable errorInfo = errorInfoRef;
            // Generate and report a null pointer exception if this
            // method is called with a null argument
            if (errorInfo == null)
            {
                errorInfo = new NullPointerException();
            }

            // Error report header
            reportHeader(output, reportNr);

            // Report the error and any nested errors
            int loopCtr = 0;
            for (Throwable curErrorInfo = errorInfo; curErrorInfo != null; curErrorInfo = curErrorInfo.getCause())
            {
                if (loopCtr <= 0)
                {
                    output.println("Reported error:\n===============\n");
                }
                else
                {
                    output.println("Caused by:\n==========\n");
                }

                reportExceptionDetails(output, curErrorInfo, loopCtr == 0 ? contextInfo : null);

                ++loopCtr;
            }

            output.println("\nEND OF ERROR REPORT.");
        }
        catch (Exception exc)
        {
            exc.printStackTrace(System.err);
        }
        return logName;
    }

    private String reportProblemImpl(
        Level logLevel,
        LinStorException errorInfo,
        AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        PrintStream output = System.err;
        String logName = null;
        long reportNr = errorNr.getAndIncrement();

        // If no description of the problem is available, log the technical details of the exception
        // as an error report instead.
        String message = errorInfo.getMessage();
        String descriptionMsg = errorInfo.getDescriptionText();
        if (descriptionMsg == null)
        {
            if (message == null)
            {
                reportError(errorInfo, accCtx, client, contextInfo);
            }
            else
            {
                descriptionMsg = message;
            }
        }

        if (descriptionMsg != null)
        {
            // Error report header
            reportHeader(output, reportNr);

            // Error description/cause/correction/details report
            String causeMsg         = errorInfo.getCauseText();
            String correctionMsg    = errorInfo.getCorrectionText();
            String detailsMsg       = errorInfo.getDetailsText();

            output.println("Description:");
            AutoIndent.printWithIndent(output, AutoIndent.DEFAULT_INDENTATION, descriptionMsg);

            if (causeMsg != null)
            {
                output.println("Cause:");
                AutoIndent.printWithIndent(output, AutoIndent.DEFAULT_INDENTATION, causeMsg);
            }

            if (correctionMsg != null)
            {
                output.println("Correction:");
                AutoIndent.printWithIndent(output, AutoIndent.DEFAULT_INDENTATION, correctionMsg);
            }

            output.println();

            if (contextInfo != null)
            {
                output.println("Error context:");
                AutoIndent.printWithIndent(output, AutoIndent.DEFAULT_INDENTATION, contextInfo);
                output.println();
            }

            if (accCtx != null)
            {
                reportAccessContext(output, accCtx);
            }

            if (client != null)
            {
                reportPeer(output, client);
            }

            // Report the error and any nested errors
            int loopCtr = 0;
            for (Throwable nestedErrorInfo = errorInfo.getCause();
                 nestedErrorInfo != null;
                 nestedErrorInfo = nestedErrorInfo.getCause())
            {
                output.println("Caused by:\n==========\n");

                if (nestedErrorInfo instanceof LinStorException)
                {
                    boolean detailsAvailable = reportLinStorException(
                        output, (LinStorException) nestedErrorInfo
                    );
                    if (!detailsAvailable)
                    {
                        reportExceptionDetails(output, nestedErrorInfo, loopCtr == 0 ? contextInfo : null);
                    }
                }
                else
                {
                    // FIXME: If a message is available, report the message,
                    //        else report as error
                }

                ++loopCtr;
            }

            output.println("\nEND OF ERROR REPORT.\n");
        }
        return logName;
    }
}
