package com.linbit.linstor.core.identifier;

import com.linbit.InvalidNameException;
import com.linbit.GenericName;
import com.linbit.Checks;

/**
 * Valid name of a linstor node
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NodeName extends GenericName
{
    public NodeName(String nodeName) throws InvalidNameException
    {
        super(nodeName);
        Checks.hostNameCheck(nodeName);
    }
}
