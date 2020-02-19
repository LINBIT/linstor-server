package com.linbit.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class LocalInetAddresses
{
    public static final Set<String> LOCAL_ADDRESSES;

    static
    {
        Set<String> tmp = new HashSet<>(Arrays.asList("127.0.0.1", "127.0.1.1", "::1"));
        try
        {
            InetAddress[] addresses = InetAddress
                .getAllByName(InetAddress.getLocalHost().getCanonicalHostName());
            for (InetAddress addr : addresses)
            {
                tmp.add(addr.getHostAddress());
            }
        }
        catch (UnknownHostException ignored)
        {
        }
        LOCAL_ADDRESSES = Collections.unmodifiableSet(tmp);
    }
}
