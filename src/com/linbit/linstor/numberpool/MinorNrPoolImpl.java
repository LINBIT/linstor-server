package com.linbit.linstor.numberpool;

import com.linbit.Checks;
import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;

import java.util.regex.Matcher;

public class MinorNrPoolImpl implements MinorNrPool
{
    public static final String PROPSCON_KEY_MINOR_NR_RANGE = "minorNrRange";

    public static final String MINOR_NR_IN_USE_EXC_FORMAT =
        "Minor number %d is already in use";

    // we will load the ranges from the database, but if the database contains
    // invalid ranges (e.g. -1 for port), we will fall back to these defaults
    private static final int DEFAULT_MINOR_NR_MIN = 1000;
    private static final int DEFAULT_MINOR_NR_MAX = 49999;

    private final Props ctrlConf;
    private final NumberPool minorNrPool;

    private int minorNrRangeMin;
    private int minorNrRangeMax;

    public MinorNrPoolImpl(Props ctrlConfRef)
    {
        ctrlConf = ctrlConfRef;

        minorNrPool = new BitmapPool(MinorNumber.MINOR_NR_MAX + 1);
    }

    @Override
    public void reloadRange()
    {
        String strRange;
        try
        {
            strRange = ctrlConf.getProp(PROPSCON_KEY_MINOR_NR_RANGE);
            Matcher matcher;
            boolean useDefaults = true;

            if (strRange != null)
            {
                matcher = Controller.RANGE_PATTERN.matcher(strRange);
                if (matcher.find())
                {
                    try
                    {
                        minorNrRangeMin = Integer.parseInt(matcher.group("min"));
                        minorNrRangeMax = Integer.parseInt(matcher.group("max"));

                        MinorNumber.minorNrCheck(minorNrRangeMin);
                        MinorNumber.minorNrCheck(minorNrRangeMax);
                        useDefaults = false;
                    }
                    catch (ValueOutOfRangeException | NumberFormatException ignored)
                    {
                    }
                }
            }
            if (useDefaults)
            {
                minorNrRangeMin = DEFAULT_MINOR_NR_MIN;
                minorNrRangeMax = DEFAULT_MINOR_NR_MAX;
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
    public int getRangeMin()
    {
        return minorNrRangeMin;
    }

    @Override
    public int getRangeMax()
    {
        return minorNrRangeMax;
    }

    @Override
    public void allocate(int nr)
        throws ValueOutOfRangeException, ValueInUseException
    {
        Checks.genericRangeCheck(nr, minorNrRangeMin, minorNrRangeMax, MinorNumber.MINOR_NR_EXC_FORMAT);
        synchronized (minorNrPool)
        {
            if (minorNrPool.isAllocated(nr))
            {
                throw new ValueInUseException(String.format(MINOR_NR_IN_USE_EXC_FORMAT, nr));
            }
            minorNrPool.allocate(nr);
        }
    }

    @Override
    public int getFreeMinorNr() throws ExhaustedPoolException
    {
        synchronized (minorNrPool)
        {
            return minorNrPool.autoAllocate(
                minorNrRangeMin,
                minorNrRangeMax
            );
        }
    }
}
