package com.linbit.linstor.core.identifier;

import com.linbit.Checks;
import com.linbit.GenericName;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;

/**
 * Name of a network interface
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NetInterfaceName extends GenericName
{
    public static final int MIN_LENGTH = 3;
    public static final int MAX_LENGTH = 32;

    public static final byte[] VALID_CHARS = {'_'};
    public static final byte[] VALID_INNER_CHARS = {'_', '-'};

    public static final NetInterfaceName DEFAULT_NET_INTERFACE_NAME;
    static
    {
        try
        {
            DEFAULT_NET_INTERFACE_NAME = new NetInterfaceName("default");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "Invalid generation of DEFAULT_NET_INTERFACE_NAME in " + NetInterfaceName.class.getSimpleName(),
                nameExc
            );
        }
    }

    public NetInterfaceName(String niName) throws InvalidNameException
    {
        super(niName);
        Checks.nameCheck(niName, MIN_LENGTH, MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }
}
