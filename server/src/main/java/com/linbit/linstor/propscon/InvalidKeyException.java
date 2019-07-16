package com.linbit.linstor.propscon;

import com.linbit.linstor.LinStorRuntimeException;

/**
 * Thrown to indicate an invalid PropsContainer key
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class InvalidKeyException extends LinStorRuntimeException
{
    private static final long serialVersionUID = -8876984313356533830L;

    public final String invalidKey;

    public InvalidKeyException(String invalidKeyValue)
    {
        super("Used key is invalid: " + invalidKeyValue);
        invalidKey = invalidKeyValue;
    }
}
