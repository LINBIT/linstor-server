package com.linbit.linstor.debug;

import com.linbit.AutoIndent;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.security.AccessContext;

import java.io.PrintStream;
import java.util.Map;

/**
 * Displays a list of currently available remote API calls
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayApis extends BaseDebugCmd
{
    public CmdDisplayApis()
    {
        super(
            new String[]
            {
                "DspApi"
            },
            "Display currently available remote API calls",
            "Displays a list of the API calls that are currently available for calls through\n" +
            "network communications services",
            null,
            null,
            false
        );
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
        int count = 0;
        for (ApiCall apiObj : cmnDebugCtl.getApiCallObjects().values())
        {
            debugOut.printf("\u001b[1;37m%s\u001b[0m\n", apiObj.getName());
            String description = apiObj.getDescription();
            if (description != null)
            {
                AutoIndent.printWithIndent(debugOut, 4, description);
            }
            AutoIndent.printWithIndent(
                debugOut, 4,
                "Provider: " + apiObj.getClass().getCanonicalName()
            );
            ++count;
        }
        if (count == 0)
        {
            debugOut.println("No APIs are currently available.");
        }
        else
        if (count == 1)
        {
            debugOut.println("1 API is currently available");
        }
        else
        {
            debugOut.printf("%d APIs are currently available\n", count);
        }
    }
}
