package com.linbit.linstor.numberpool;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;

import java.util.regex.Matcher;

public class TcpPortPoolImpl implements TcpPortPool
{
    public static final String PROPSCON_KEY_TCP_PORT_RANGE = "tcpPortRange";

    // we will load the ranges from the database, but if the database contains
    // invalid ranges (e.g. -1 for port), we will fall back to these defaults
    private static final int DEFAULT_TCP_PORT_MIN = 7000;
    private static final int DEFAULT_TCP_PORT_MAX = 7999;

    private final Props ctrlConf;
    private final NumberPool tcpPortPool;

    private int tcpPortRangeMin;
    private int tcpPortRangeMax;

    public TcpPortPoolImpl(Props ctrlConfRef)
    {
        ctrlConf = ctrlConfRef;

        tcpPortPool = new BitmapPool(TcpPortNumber.PORT_NR_MAX + 1);
    }

    @Override
    public void reloadRange()
    {
        String strRange;
        try
        {
            strRange = ctrlConf.getProp(PROPSCON_KEY_TCP_PORT_RANGE);
            Matcher matcher;
            boolean useDefaults = true;

            if (strRange != null)
            {
                matcher = Controller.RANGE_PATTERN.matcher(strRange);
                if (matcher.find())
                {
                    try
                    {
                        tcpPortRangeMin = Integer.parseInt(matcher.group("min"));
                        tcpPortRangeMax = Integer.parseInt(matcher.group("max"));

                        TcpPortNumber.tcpPortNrCheck(tcpPortRangeMin);
                        TcpPortNumber.tcpPortNrCheck(tcpPortRangeMax);
                        useDefaults = false;
                    }
                    catch (ValueOutOfRangeException | NumberFormatException ignored)
                    {
                    }
                }
            }
            if (useDefaults)
            {
                tcpPortRangeMin = DEFAULT_TCP_PORT_MIN;
                tcpPortRangeMax = DEFAULT_TCP_PORT_MAX;
            }
        }
        catch (InvalidKeyException invldKeyExc)
        {
            throw new ImplementationError(
                "Controller configuration key was invalid: " + invldKeyExc.invalidKey,
                invldKeyExc
            );
        }
    }

    @Override
    public void allocate(int nr)
    {
        synchronized (tcpPortPool)
        {
            tcpPortPool.allocate(nr);
        }
    }

    @Override
    public int getFreeTcpPort() throws ExhaustedPoolException
    {
        synchronized (tcpPortPool)
        {
            return tcpPortPool.autoAllocate(
                tcpPortRangeMin,
                tcpPortRangeMax
            );
        }
    }
}
