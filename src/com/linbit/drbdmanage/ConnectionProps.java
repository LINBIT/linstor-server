package com.linbit.drbdmanage;

import java.sql.SQLException;

import com.linbit.drbdmanage.propscon.InvalidKeyException;
import com.linbit.drbdmanage.propscon.InvalidValueException;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

public class ConnectionProps
{
    private final Props nodeProps;
    private final Props resProps;
    private final Props volProps;

    public ConnectionProps(
        AccessContext accCtx,
        NodeConnection nodeConnectionDefinition,
        ResourceConnection resourceConnectionDefinition,
        VolumeConnection volumeConnectionDefinition
    )
        throws AccessDeniedException
    {
        if (nodeConnectionDefinition == null)
        {
            nodeProps = null;
        }
        else
        {
            nodeProps = nodeConnectionDefinition.getProps(accCtx);
        }
        if (resourceConnectionDefinition == null)
        {
            resProps = null;
        }
        else
        {
            resProps = resourceConnectionDefinition.getProps(accCtx);
        }
        if (volumeConnectionDefinition == null)
        {
            volProps = null;
        }
        else
        {
            volProps = volumeConnectionDefinition.getProps(accCtx);
        }
    }

    public String getProp(String key, String namespace) throws InvalidKeyException
    {
        String value = volProps.getProp(key, namespace);
        if (value == null)
        {
            value = resProps.getProp(key, namespace);
            if (value == null)
            {
                value = nodeProps.getProp(key, namespace);
            }
        }
        return value;
    }

    public String getProp(String key) throws InvalidKeyException
    {
        String value = volProps.getProp(key);
        if (value == null)
        {
            value = resProps.getProp(key);
            if (value == null)
            {
                value = nodeProps.getProp(key);
            }
        }
        return value;
    }

    public void setNodeConnectionProp(String key, String value) throws AccessDeniedException, InvalidKeyException, InvalidValueException, SQLException
    {
        nodeProps.setProp(key, value);
    }

    public void setNodeConnectionProp(String key, String value, String namespace) throws AccessDeniedException, InvalidKeyException, InvalidValueException, SQLException
    {
        nodeProps.setProp(key, value, namespace);
    }

    public void setResourceConnectionProp(String key, String value) throws AccessDeniedException, InvalidKeyException, InvalidValueException, SQLException
    {
        resProps.setProp(key, value);
    }

    public void setResourceConnectionProp(String key, String value, String namespace) throws AccessDeniedException, InvalidKeyException, InvalidValueException, SQLException
    {
        resProps.setProp(key, value, namespace);
    }

    public void setVolumeConnectionProp(String key, String value) throws AccessDeniedException, InvalidKeyException, InvalidValueException, SQLException
    {
        volProps.setProp(key, value);
    }

    public void setVolumeConnectionProp(String key, String value, String namespace) throws AccessDeniedException, InvalidKeyException, InvalidValueException, SQLException
    {
        volProps.setProp(key, value, namespace);
    }
}
