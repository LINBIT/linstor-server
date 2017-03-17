package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;

/**
 * Displays information about the Controller's system services
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayConnections extends BaseControllerDebugCmd
{
    public CmdDisplayConnections()
    {
        super(
            new String[]
            {
                "DspCon"
            },
            "Display connections",
            "Displays a table with information about connected clients",
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
    ) throws Exception
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
