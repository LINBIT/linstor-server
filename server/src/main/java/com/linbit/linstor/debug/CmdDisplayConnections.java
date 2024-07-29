package com.linbit.linstor.debug;

import com.linbit.AutoIndent;
import com.linbit.ServiceName;
import com.linbit.linstor.ControllerPeerCtx;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.Identity;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.PrivilegeSet;
import com.linbit.linstor.security.Role;
import com.linbit.linstor.security.SecurityType;

import javax.inject.Inject;

import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
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
    private static final String PRM_DETAIL_NAME     = "DETAIL";
    private static final String PRM_DETAIL_DFLT     = "DEFAULT";
    private static final String PRM_DETAIL_ID       = "ID";
    private static final String PRM_DETAIL_STATS    = "STATS";
    private static final String PRM_DETAIL_CONN     = "CONN";
    private static final String PRM_DETAIL_CTXT     = "CONTEXT";
    private static final String PRM_DETAIL_PRIVS    = "PRIVS";
    private static final String PRM_DETAIL_PENDING  = "PENDING";
    private static final String PRM_DETAIL_FULL     = "FULL";

    private static final String PRM_CONNECTOR_MATCH = "CONNECTOR";

    private static final String PRM_ADDRESS_MATCH = "ADDRESS";

    private static final String PRM_CONNID_MATCH = "CONNID";

    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_DETAIL_NAME,
            "The level of detail to display\n" +
            "    DEFAULT\n" +
            "        Displays the peer address along with statistics\n" +
            "    ID\n" +
            "        Displays connection IDs\n" +
            "    STATS\n" +
            "        Displays queueing and traffic statistics\n" +
            "    CONN\n" +
            "        Displays additional information about the connection\n" +
            "    CONTEXT\n" +
            "        Displays access contexts\n" +
            "    PRIVS\n" +
            "        Displays the privilege sets associated with the\n" +
            "        connection's access context\n" +
            "    PENDING\n" +
            "        Displays pending connections (connections in the connect phase)\n" +
            "        This option implies the ID option, because pending connections can be in a state\n" +
            "        where the endpoint addresses are unknown, so the connection can only be identified\n" +
            "        by its connection ID\n" +
            "    FULL\n" +
            "        Displays all available information\n"
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

    private final CoreModule.PeerMap peerMap;

    @Inject
    public CmdDisplayConnections(CoreModule.PeerMap peerMapRef)
    {
        super(
            new String[]
            {
                "DspCon"
            },
            "Display connections",
            "Displays a table with information about connected clients",
            PARAMETER_DESCRIPTIONS,
            null
        );

        peerMap = peerMapRef;
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    ) throws Exception
    {
        boolean detailId        = false;
        boolean detailStats     = false;
        boolean detailConn      = false;
        boolean detailContext   = false;
        boolean detailPrivs     = false;
        boolean detailPending   = false;

        try
        {
            String prmDetail = parameters.get(PRM_DETAIL_NAME);
            if (prmDetail != null)
            {
                InvalidDetailsException detailsExc = null;
                prmDetail = prmDetail.toUpperCase();
                StringTokenizer prmDetailTokens = new StringTokenizer(prmDetail, ",");
                while (prmDetailTokens.hasMoreTokens())
                {
                    String curToken = prmDetailTokens.nextToken().trim();
                    switch (curToken)
                    {
                        case PRM_DETAIL_ID:
                            detailId = true;
                            break;
                        case PRM_DETAIL_STATS:
                            detailStats = true;
                            break;
                        case PRM_DETAIL_CONN:
                            detailConn = true;
                            break;
                        case PRM_DETAIL_CTXT:
                            detailContext = true;
                            break;
                        case PRM_DETAIL_PRIVS:
                            detailPrivs = true;
                            break;
                        case PRM_DETAIL_PENDING:
                            detailPending = true;
                            detailId = true;
                            break;
                        case PRM_DETAIL_FULL:
                            detailId = true;
                            detailStats = true;
                            detailConn = true;
                            detailContext = true;
                            detailPending = true;
                            detailPrivs = true;
                            break;
                        case PRM_DETAIL_DFLT:
                            // fall-through
                            break;
                        default:
                            if (detailsExc == null)
                            {
                                detailsExc = new InvalidDetailsException();
                            }
                            detailsExc.addInvalid(curToken);
                            break;
                    }
                }
                if (detailsExc != null)
                {
                    throw detailsExc;
                }
            }

            Map<String, Peer> peerList = getAllPeers();

            Matcher connSvcMatch = createMatcher(parameters, PRM_CONNECTOR_MATCH, debugErr);
            Matcher addrMatch = createMatcher(parameters, PRM_ADDRESS_MATCH, debugErr);
            Matcher connIdMatch = createMatcher(parameters, PRM_CONNID_MATCH, debugErr);

            if (peerList.size() > 0)
            {
                debugOut.printf(
                    "%-46s \u2194 %-46s\n",
                    "Local address",
                    "Remote address"
                );
                printSectionSeparator(debugOut);

                int count = 0;
                for (Peer curPeer : peerList.values())
                {
                    if (detailPending || curPeer.isConnected(false))
                    {
                        String connId = curPeer.getId();
                        String localAddress = "<unknown>";
                        {
                            InetSocketAddress sockAddr = curPeer.localAddress();
                            if (sockAddr != null)
                            {
                                InetAddress inetAddr = sockAddr.getAddress();
                                if (inetAddr != null)
                                {
                                    localAddress = inetAddr.getHostAddress() + ":" + sockAddr.getPort();
                                }
                            }
                        }
                        String peerAddress = "<unknown>";
                        {
                            InetSocketAddress sockAddr = curPeer.peerAddress();
                            if (sockAddr != null)
                            {
                                InetAddress inetAddr = sockAddr.getAddress();
                                if (inetAddr != null)
                                {
                                    peerAddress = inetAddr.getHostAddress() + ":" + sockAddr.getPort();
                                }
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
                            match(addrMatch, peerAddress) &&
                            match(connIdMatch, connId)
                        );

                        if (selected)
                        {
                            AccessContext peerAccCtx = curPeer.getAccessContext();
                            debugOut.printf(
                                "%-46s \u2194 %-46s\n",
                                localAddress,
                                peerAddress
                            );
                            if (detailStats)
                            {
                                debugOut.printf(
                                    "    MsgRecv: %8d   MsgSent: %8d   OutQ: %5d  QCap: %5d  " +
                                    "RecvPeakSz: %8d  SentPeakSz: %8d\n",
                                    curPeer.msgRecvCount(), curPeer.msgSentCount(),
                                    curPeer.outQueueCount(), curPeer.outQueueCapacity(),
                                    curPeer.msgRecvMaxSize(), curPeer.msgSentMaxSize()
                                );
                            }
                            if (detailId)
                            {
                                debugOut.printf(
                                    "    Id:         %-64s\n",
                                    curPeer.getId()
                                );
                                Node peerNode = curPeer.getNode();
                                if (peerNode != null)
                                {
                                    NodeName peerNodeName = peerNode.getName();
                                    debugOut.printf(
                                        "    Peer:       Satellite on %s\n",
                                        peerNodeName.displayValue
                                    );
                                }
                                Object peerCtx = curPeer.getAttachment();
                                if (peerCtx instanceof com.linbit.linstor.ControllerPeerCtx)
                                {
                                    ControllerPeerCtx ctrlPeerCtx = (ControllerPeerCtx) peerCtx;
                                    DebugConsole dbgCnsl = ctrlPeerCtx.getDebugConsole();
                                    if (dbgCnsl != null)
                                    {
                                        debugOut.println("    Debug console attached");
                                    }
                                }
                            }
                            if (detailConn)
                            {
                                debugOut.printf(
                                    "    Connector:  %-24s\n",
                                    connector
                                );
                            }
                            if (detailContext)
                            {
                                Identity peerIdentity = peerAccCtx.getIdentity();
                                Role peerRole = peerAccCtx.getRole();
                                SecurityType peerDomain = peerAccCtx.getDomain();
                                debugOut.printf(
                                    "    Identity:   %-24s Role: %-24s\n" +
                                    "    Security domain: %-24s\n",
                                    peerIdentity, peerRole, peerDomain
                                );
                            }
                            if (detailPrivs)
                            {
                                debugOut.println("    Limit privileges:");
                                printPrivString(debugOut, peerAccCtx.getLimitPrivs());
                                debugOut.println("    Effective privileges:");
                                printPrivString(debugOut, peerAccCtx.getEffectivePrivs());
                            }
                            ++count;
                        }
                    }
                }

                printSectionSeparator(debugOut);
                if (connIdMatch != null || connSvcMatch != null || addrMatch != null)
                {
                    String countFormat;
                    if (count == 1)
                    {
                        countFormat = "%d connection listed\n";
                    }
                    else
                    {
                        countFormat = "%d connections listed\n";
                    }
                    debugOut.printf(countFormat, count);
                }
                int total = peerList.size();
                String totalFormat;
                if (total == 1)
                {
                    totalFormat = "%d active connection\n";
                }
                else
                {
                    totalFormat = "%d active connections\n";
                }
                debugOut.printf(totalFormat, total);
            }
            else
            {
                debugOut.println("No active connections");
            }
        }
        catch (InvalidDetailsException detailsExc)
        {
            printError(
                debugErr,
                String.format(
                    "The following values are not valid for the parameter %s:\n%s",
                    PRM_DETAIL_NAME, detailsExc.list()
                ),
                null,
                "Enter a valid value for the parameter.",
                String.format(
                    "Valid values are:\n" +
                    "    " + PRM_DETAIL_DFLT + "\n" +
                    "    " + PRM_DETAIL_ID + "\n" +
                    "    " + PRM_DETAIL_CONN + "\n" +
                    "    " + PRM_DETAIL_CTXT + "\n" +
                    "    " + PRM_DETAIL_PRIVS + "\n" +
                    "    " + PRM_DETAIL_FULL
                )
            );
        }
        catch (PatternSyntaxException patternExc)
        {
            // Error reported already, no-op
        }
    }

    private @Nullable Matcher createMatcher(
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
                Pattern ptrn = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
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

    @SuppressWarnings("checkstyle:magicnumber")
    private void  printPrivString(PrintStream output, PrivilegeSet privSet)
    {
        List<Privilege> privList = privSet.getEnabledPrivileges();
        if (privList.isEmpty())
        {
            AutoIndent.printWithIndent(output, 8, "NONE");
        }
        else
        {
            for (Privilege priv : privList)
            {
                AutoIndent.printWithIndent(output, 8, priv.name);
            }
        }
    }

    public Map<String, Peer> getAllPeers()
    {
        TreeMap<String, Peer> peerMapCpy = new TreeMap<>();
        synchronized (peerMap)
        {
            peerMapCpy.putAll(peerMap);
        }
        return peerMapCpy;
    }
}
