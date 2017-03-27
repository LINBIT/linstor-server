package com.linbit.drbdmanage.debug;

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

/**
 * Displays information about the Controller's system services
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayConnections extends BaseControllerDebugCmd
{
    private static final String PRM_DETAIL_NAME = "DETAIL";
    private static final String PRM_DETAIL_DFLT = "DEFAULT";
    private static final String PRM_DETAIL_FULL = "FULL";

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
                debugErr.printf(
                    String.format(
                        "Error:\n" +
                        "    The value '%s' is not valid for the parameter '%s'.\n" +
                        "Correction:\n" +
                        "    Enter a valid value for the parameter.\n" +
                        "    Valid values are '%s' and '%s'.\n",
                        prmDetail, PRM_DETAIL_NAME,
                        PRM_DETAIL_DFLT, PRM_DETAIL_FULL
                    )
                );
                detail = DetailLevel.INVALID;
            }
        }

        if (detail != DetailLevel.INVALID)
        {
            Map<String, Peer> peerList = debugCtl.getAllPeers();

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
                                "  Id: %-64s\n  QCap: %4d\n" +
                                "  Identity: %-24s Role: %-24s Domain: %-24s\n",
                                curPeer.getId(), curPeer.outQueueCapacity(),
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

                debugOut.println(ruler);
                debugOut.printf("%d connections\n", count);
            }
            else
            {
                debugOut.println("There are no connections to clients registered at this time.");
            }
        }
    }
}
