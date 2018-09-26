package com.linbit.linstor.debug;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;

import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;

public class CmdDisplayReport extends BaseDebugCmd
{
    public static final String PRM_RPT_ID = "ID";
    public static final int BUFFER_INITIAL_SIZE = 4096;

    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_RPT_ID,
            "Report id of the report file to display"
        );
    }

    private final ErrorReporter errorReporter;

    @Inject
    public CmdDisplayReport(ErrorReporter errorReporterRef)
    {
        super(
            new String[]
            {
                "DspRpt"
            },
            "Display report",
            "Displays an error report file",
            PARAMETER_DESCRIPTIONS,
            null
        );
        errorReporter = errorReporterRef;
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    )
        throws Exception
    {
        String prmReportId = parameters.get(PRM_RPT_ID);
        if (prmReportId != null)
        {
            String instanceId = null;
            String formattedErrorId = null;
            String reportFile = null;
            {
                String prmErrorId;
                // If the reportId consists of only an error number,
                // default to the instanceId of the current linstor instance
                {
                    int splitIdx = prmReportId.lastIndexOf('-');
                    if (prmReportId.indexOf('-') != -1)
                    {
                        instanceId = prmReportId.substring(0, splitIdx).toUpperCase();
                        prmErrorId = prmReportId.substring(splitIdx + 1, prmReportId.length());
                    }
                    else
                    {
                        instanceId = errorReporter.getInstanceId();
                        prmErrorId = prmReportId;
                    }
                }
                try
                {
                    formattedErrorId = getErrorId(prmErrorId);
                    reportFile = String.format(
                        "%s/ErrorReport-%s-%s.log",
                        errorReporter.getLogDirectory(), instanceId, formattedErrorId
                    );
                }
                catch (NumberFormatException nfExc)
                {
                    printError(
                        debugErr,
                        "The specified error report number is not valid.",
                        String.format("The error number '%s' is not a numeric value.", prmErrorId),
                        "Reenter the command using a valid numeric error number.",
                        null
                    );
                }
            }

            if (reportFile != null)
            {
                StringBuilder outputBuffer = new StringBuilder();
                try (
                    BufferedReader fileIn = new BufferedReader(
                        new InputStreamReader(new FileInputStream(reportFile))
                    )
                )
                {
                    // Read the entire file
                    String line;
                    do
                    {
                        line = fileIn.readLine();
                        if (line != null)
                        {
                            outputBuffer.append(line);
                            outputBuffer.append('\n');
                        }
                    }
                    while (line != null);

                    // If the entire file has been read without encountering errors,
                    // output the contents
                    debugOut.print(outputBuffer.toString());
                    debugOut.flush();
                }
                catch (FileNotFoundException fnfExc)
                {
                    String detailMsg = "The generated log file path is:\n" + reportFile;
                    {
                        String ioErrMsg = fnfExc.getMessage();
                        if (ioErrMsg != null)
                        {
                            detailMsg = String.format(
                                "The error reported by the operating system was:\n%s",
                                fnfExc.getMessage()
                            );
                        }
                    }
                    printError(
                        debugErr,
                        String.format(
                            "No log file for an error report with instance id %s, error number %s was found",
                            instanceId, formattedErrorId
                        ),
                        null,
                        "Make sure that the specified report ID is correct and that the corresponding\n" + "" +
                        "log file exists and is accessible by the software",
                        detailMsg
                    );
                }
                catch (IOException ioExc)
                {
                    String detailMsg = "No details were reported by the runtime environment or by " +
                    "the operating system.";
                    {
                        String ioErrMsg = ioExc.getMessage();
                        if (ioErrMsg != null)
                        {
                            detailMsg = String.format(
                                "The I/O error reported by the operating system was:\n%s",
                                ioExc.getMessage()
                            );
                        }
                    }
                    printError(
                        debugErr,
                        "Reading the log file failed due to an I/O error.",
                        detailMsg,
                        null,
                        null
                    );
                }
            }
        }
        else
        {
            printMissingParamError(debugErr, PRM_RPT_ID);
        }
    }

    private String getErrorId(String nrText)
        throws NumberFormatException
    {
        long errorNr = Long.parseLong(nrText);
        return String.format("%06d", errorNr);
    }
}
