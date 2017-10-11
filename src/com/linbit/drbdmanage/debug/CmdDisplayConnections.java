package com.linbit.drbdmanage.debug;

import com.linbit.AutoIndent;
import com.linbit.ServiceName;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.Identity;
import com.linbit.drbdmanage.security.Privilege;
import com.linbit.drbdmanage.security.PrivilegeSet;
import com.linbit.drbdmanage.security.Role;
import com.linbit.drbdmanage.security.SecurityType;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.LinkedList;
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
    private static final String PRM_DETAIL_CONN     = "CONN";
    private static final String PRM_DETAIL_CTXT     = "CONTEXT";
    private static final String PRM_DETAIL_PRIVS    = "PRIVS";
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
            "    CONN\n" +
            "        Displays additional information about the connection\n" +
            "    CONTEXT\n" +
            "        Displays access contexts\n" +
            "    PRIVS\n" +
            "        Displays the privilege sets associated with the\n" +
            "        connection's access context\n" +
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
        boolean detail_id = false;
        boolean detail_conn = false;
        boolean detail_ctxt = false;
        boolean detail_privs = false;

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
                            detail_id = true;
                            break;
                        case PRM_DETAIL_CONN:
                            detail_conn = true;
                            break;
                        case PRM_DETAIL_CTXT:
                            detail_ctxt = true;
                            break;
                        case PRM_DETAIL_PRIVS:
                            detail_privs = true;
                            break;
                        case PRM_DETAIL_FULL:
                            detail_id = true;
                            detail_conn = true;
                            detail_ctxt = true;
                            detail_privs = true;
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

            Map<String, Peer> peerList = cmnDebugCtl.getAllPeers();

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
                        AccessContext peerAccCtx = curPeer.getAccessContext();
                        debugOut.printf(
                            "%-46s %5d %8d %8d\n",
                            address,
                            curPeer.outQueueCount(),
                            curPeer.msgRecvCount(),
                            curPeer.msgSentCount()
                        );
                        if (detail_id)
                        {
                            debugOut.printf(
                                "    Id:        %-64s\n",
                                curPeer.getId()
                            );
                        }
                        if (detail_conn)
                        {
                            debugOut.printf(
                                "    Connector: %-24s QCap: %4d\n",
                                connector, curPeer.outQueueCapacity()
                            );
                        }
                        if (detail_ctxt)
                        {
                            Identity peerIdentity = peerAccCtx.getIdentity();
                            Role peerRole = peerAccCtx.getRole();
                            SecurityType peerDomain = peerAccCtx.getDomain();
                            debugOut.printf(
                                "    Identity:  %-24s Role: %-24s\n" +
                                "    Security domain: %-24s\n",
                                peerIdentity, peerRole, peerDomain
                            );
                        }
                        if (detail_privs)
                        {
                            debugOut.println("    Limit privileges:");
                            printPrivString(debugOut, peerAccCtx.getLimitPrivs());
                            debugOut.println("    Effective privileges:");
                            printPrivString(debugOut, peerAccCtx.getEffectivePrivs());
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

    private void  printPrivString(PrintStream output, PrivilegeSet privSet)
    {
        List<Privilege> privList = privSet.getEnabledPrivileges();
        if (privList.size() == 0)
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

    private static class InvalidDetailsException extends Exception
    {
        private final List<String> invalidDetails = new LinkedList<>();

        void addInvalid(String value)
        {
            invalidDetails.add(value);
        }

        String list()
        {
            StringBuilder invList = new StringBuilder();
            for (String detail : invalidDetails)
            {
                if (invList.length() > 0)
                {
                    invList.append('\n');
                }
                invList.append(detail);
            }
            return invList.toString();
        }
    }
}
