package com.linbit.linstor.debug;

import com.linbit.AutoIndent;
import com.linbit.linstor.api.protobuf.ApiCallDescriptor;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;

/**
 * Displays a list of currently available remote API calls
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayApis extends BaseDebugCmd
{
    private final Map<String, ApiCallDescriptor> apiCallDescriptors;

    @Inject
    public CmdDisplayApis(Map<String, ApiCallDescriptor> apiCallDescriptorsRef)
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
            null
        );

        apiCallDescriptors = apiCallDescriptorsRef;
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
        TreeMap<String, ApiCallDescriptor> sortedApiObjList = new TreeMap<>();
        sortedApiObjList.putAll(apiCallDescriptors);
        int count = 0;
        for (Map.Entry<String, ApiCallDescriptor> apiObjEntry : sortedApiObjList.entrySet())
        {
            debugOut.printf("\u001b[1;37m%s\u001b[0m\n", apiObjEntry.getKey());
            ApiCallDescriptor apiObj = apiObjEntry.getValue();
            String description = apiObj.getDescription();
            AutoIndent.printWithIndent(debugOut, AutoIndent.DEFAULT_INDENTATION, description);
            AutoIndent.printWithIndent(
                debugOut, AutoIndent.DEFAULT_INDENTATION,
                "Provider: " + apiObj.getClazz().getCanonicalName() + "\n" +
                "Call policy: " + (apiObj.requiresAuth() ? "Authenticated identity" : "Anonymous/PUBLIC")
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
