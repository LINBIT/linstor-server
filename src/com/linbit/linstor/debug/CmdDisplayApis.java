package com.linbit.linstor.debug;

import javax.inject.Inject;
import com.linbit.AutoIndent;
import com.linbit.linstor.api.protobuf.ApiCallDescriptor;
import com.linbit.linstor.proto.CommonMessageProcessor;
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
    private final CommonMessageProcessor commonMessageProcessor;

    @Inject
    public CmdDisplayApis(CommonMessageProcessor commonMessageProcessorRef)
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

        commonMessageProcessor = commonMessageProcessorRef;
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
        for (ApiCallDescriptor apiObj : commonMessageProcessor.getApiCallDescriptors().values())
        {
            debugOut.printf("\u001b[1;37m%s\u001b[0m\n", apiObj.getName());
            String description = apiObj.getDescription();
            if (description != null)
            {
                AutoIndent.printWithIndent(debugOut, 4, description);
            }
            AutoIndent.printWithIndent(
                debugOut, 4,
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
