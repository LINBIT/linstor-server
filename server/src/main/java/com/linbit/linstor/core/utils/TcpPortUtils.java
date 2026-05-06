package com.linbit.linstor.core.utils;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.types.TcpPortNumber;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TcpPortUtils
{
    private TcpPortUtils()
    {
    }

    /**
     * Returns a possibly empty list of ports that are currently blocked as int. {@code null} as {@code ipAddr} will be
     * interpreted as 0.0.0.0
     */
    public static List<Integer> getBlockedPortsAsIntList(
        @Nullable InetAddress ipAddr,
        @Nullable Collection<TcpPortNumber> collectionRef
    )
    {
        List<Integer> ret = new ArrayList<>();
        if (collectionRef != null)
        {
            for (TcpPortNumber port : collectionRef)
            {
                if (!isTcpPortAvailable(ipAddr, port))
                {
                    ret.add(port.value);
                }
            }
        }
        return ret;
    }

    /**
     * Returns true/false whether or not the given TCP port is available on the given IP. {@code null} as an IP will
     * be interpreted as 0.0.0.0
     */
    public static boolean isTcpPortAvailable(@Nullable InetAddress ipAddr, TcpPortNumber port)
    {
        return isTcpPortAvailable(ipAddr, port.value);
    }

    /**
     * Returns true/false whether or not the given TCP port is available on the given IP. {@code null} as an IP will
     * be interpreted as 0.0.0.0
     */
    public static boolean isTcpPortAvailable(@Nullable InetAddress ipAddr, int port)
    {
        boolean portAvailable = false;
        try (ServerSocket ss = new ServerSocket(port))
        {
            ss.setReuseAddress(true);
            portAvailable = true;
        }
        catch (IOException ignored)
        {
        }
        return portAvailable;
    }
}
