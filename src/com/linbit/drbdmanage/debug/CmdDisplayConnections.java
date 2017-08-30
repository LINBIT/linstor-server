package com.linbit.drbdmanage.debug;

import com.linbit.ServiceName;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.Identity;
import com.linbit.drbdmanage.security.Role;
import com.linbit.drbdmanage.security.SecurityLevel;
import com.linbit.drbdmanage.security.SecurityType;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Displays information about network connections
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayConnections extends BaseDebugCmd
{
    private static final String PRM_DETAIL_NAME = "DETAIL";
    private static final String PRM_DETAIL_DFLT = "DEFAULT";
    private static final String PRM_DETAIL_FULL = "FULL";

    private static final String PRM_CONNECTOR_MATCH = "CONNECTOR";

    private static final String PRM_ADDRESS_MATCH = "ADDRESS";

    private static final String PRM_CONNID_MATCH = "CONNID";

    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private enum DetailLevel
    {
        DEFAULT,
        FULL,
        INVALID
    }

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_DETAIL_NAME,
            "The level of detail to display; '" + PRM_DETAIL_DFLT +
            "' or '" + PRM_DETAIL_FULL + "'"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_CONNECTOR_MATCH,
            "Filter pattern to apply to the name of the connector that handles the connection.\n" +
            "Only connections that are handled by a connector with a matching instance name will be listed."
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_ADDRESS_MATCH,
            "Filter pattern to apply to the remote (peer) address.\n" +
            "Only connections originating from addresses that match the filter will be listed."
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_CONNID_MATCH,
            "Filter pattern to apply to the connection id.\n" +
            "Only connections with a matching connection ID will be listed."
        );
    }

    public CmdDisplayConnections()
    {
        super(
            new String[]
            {
                "DspCon"
            },
            "Display connections",
            "Displays a table with information about connected clients",
            PARAMETER_DESCRIPTIONS,
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
    ) throws Exception
    {
        DetailLevel detail = DetailLevel.DEFAULT;

        String prmDetail = parameters.get(PRM_DETAIL_NAME);
        if (prmDetail != null)
        {
            prmDetail = prmDetail.toUpperCase();
            if (prmDetail.equals(PRM_DETAIL_FULL))
            {
                detail = DetailLevel.FULL;
            }
            else
            if (prmDetail.equals(PRM_DETAIL_DFLT))
            {
                detail = DetailLevel.DEFAULT;
            }
            else
            {
                printError(
                    debugErr,
                    String.format(
                        "The value '%s' is not valid for the parameter '%s'.",
                        prmDetail, PRM_DETAIL_NAME
                    ),
                    null,
                    "Enter a valid value for the parameter.",
                    String.format(
                        "Valid values are '%s' and '%s'.",
                        PRM_DETAIL_DFLT, PRM_DETAIL_FULL
                    )
                );
                detail = DetailLevel.INVALID;
            }
        }

        if (detail != DetailLevel.INVALID)
        {
            Map<String, Peer> peerList = cmnDebugCtl.getAllPeers();

            try
            {
                Matcher connSvcMatch = createMatcher(parameters, PRM_CONNECTOR_MATCH, debugErr);
                Matcher addrMatch = createMatcher(parameters, PRM_ADDRESS_MATCH, debugErr);
                Matcher connIdMatch = createMatcher(parameters, PRM_CONNID_MATCH, debugErr);

                if (peerList.size() > 0)
                {
                    char[] rulerData = new char[78];
                    Arrays.fill(rulerData, '-');
                    String ruler = new String(rulerData);
                    debugOut.printf(
                        "%-46s %5s %8s %8s\n",
                        "Endpoint address",
                        "OutQ",
                        "MsgRecv",
                        "MsgSent"
                    );
                    debugOut.println(ruler);

                    int count = 0;
                    for (Peer curPeer : peerList.values())
                    {
                        String connId = curPeer.getId();
                        String address = "<unknown>";
                        InetSocketAddress peerAddr = curPeer.peerAddress();
                        if (peerAddr != null)
                        {
                            InetAddress inetAddr = peerAddr.getAddress();
                            if (inetAddr != null)
                            {
                                address = inetAddr.getHostAddress() + ":" + peerAddr.getPort();
                            }
                        }
                        String connector = "<unknown>";
                        {
                            ServiceName connectorInstance = curPeer.getConnectorInstanceName();
                            if (connectorInstance != null)
                            {
                                connector = connectorInstance.displayValue;
                            }
                        }

                        boolean selected = (
                            match(connSvcMatch, connector) &&
                            match(addrMatch, address) &&
                            match(connIdMatch, connId)
                        );

                        if (selected)
                        {
                            debugOut.printf(
                                "%-46s %5d %8d %8d\n",
                                address,
                                curPeer.outQueueCount(),
                                curPeer.msgRecvCount(),
                                curPeer.msgSentCount()
                            );
                            if (detail == DetailLevel.FULL)
                            {
                                AccessContext peerAccCtx = curPeer.getAccessContext();
                                Identity peerIdentity = peerAccCtx.getIdentity();
                                Role peerRole = peerAccCtx.getRole();
                                if (SecurityLevel.get() == SecurityLevel.MAC)
                                {
                                    SecurityType peerDomain = peerAccCtx.getDomain();
                                    debugOut.printf(
                                        "  Id:        %-64s\n"+
                                        "  Connector: %-24s QCap: %4d\n" +
                                        "  Identity:  %-24s Role: %-24s\n" +
                                        "  Security domain: %-24s\n",
                                        curPeer.getId(), connector,
                                        curPeer.outQueueCapacity(),
                                        peerIdentity, peerRole, peerDomain
                                    );
                                }
                                else
                                {
                                    debugOut.printf(
                                        "  %-64s QCap: %4d\n" +
                                        "Identity: %-24s Role: %-24s\n",
                                        curPeer.getId(), curPeer.outQueueCapacity(),
                                        peerIdentity, peerRole
                                    );
                                }
                            }
                            ++count;
                        }
                    }

                    debugOut.println(ruler);
                    if (count == 1)
                    {
                        debugOut.println("1 connection");
                    }
                    else
                    {
                        debugOut.printf("%d connections\n", count);
                    }
                }
                else
                {
                    debugOut.println("There are no connections registered at this time.");
                }
            }
            catch (PatternSyntaxException patternExc)
            {
                // Error reported already, no-op
            }
        }
    }

    private Matcher createMatcher(
        Map<String, String> parameters,
        String paramName,
        PrintStream debugErr
    )
        throws PatternSyntaxException
    {
        Matcher regexMatch = null;
        try
        {
            String regex = parameters.get(paramName);
            if (regex != null)
            {
                Pattern ptrn = Pattern.compile(regex);
                regexMatch = ptrn.matcher("");
            }
        }
        catch (PatternSyntaxException patternExc)
        {
            String errorCause;
            String excMessage = patternExc.getMessage();
            if (excMessage == null)
            {
                errorCause = "The regular expression library did not return an error cause description.";
            }
            else
            {
                errorCause = "The error description returned by the regular expression library is:\n" +
                             excMessage;
            }
            printError(
                debugErr,
                "The regular expression argument for the filter parameter '" + paramName + "' is invalid.",
                errorCause,
                "Reenter the command with the corrected filter parameter.",
                null
            );
            throw patternExc;
        }
        return regexMatch;
    }

    private boolean match(Matcher filterMatch, String input)
    {
        boolean result;
        if (filterMatch != null)
        {
            filterMatch.reset(input);
            result = filterMatch.find();
        }
        else
        {
            result = true;
        }
        return result;
    }
}
