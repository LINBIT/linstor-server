package com.linbit.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

public class LocalInetAddresses
{
    public static final Set<String> LOCAL_ADDRESSES;

    private LocalInetAddresses()
    {
    }

    static
    {
        Set<String> tmp = new HashSet<>(Arrays.asList("127.0.0.1", "127.0.1.1", "::1"));
        try
        {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements())
            {
                NetworkInterface netInterface = networkInterfaces.nextElement();
                Enumeration<InetAddress> inetAddresses = netInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements())
                {
                    InetAddress inetAddr = inetAddresses.nextElement();
                    tmp.add(inetAddr.getHostAddress());
                }
            }
        }
        catch (SocketException ignored)
        {
        }
        LOCAL_ADDRESSES = Collections.unmodifiableSet(tmp);
    }
}
