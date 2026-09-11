package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

/**
 * Holds the singleton system props instance.
 */
@Singleton
public class SystemConfRepositoryImpl implements SystemConfRepository
{
    private final Props ctrlConf;
    private final Props stltConf;

    @Inject
    public SystemConfRepositoryImpl(
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef,
        @Named(LinStor.SATELLITE_PROPS) Props stltConfRef
    )
    {
        ctrlConf = ctrlConfRef;
        stltConf = stltConfRef;
    }

    @Override
    public @Nullable String setCtrlProp(String key, String value, String namespace)
        throws InvalidValueException, DatabaseException, InvalidKeyException
    {
        return ctrlConf.setProp(key, value, namespace);
    }

    @Override
    public @Nullable String setStltProp(String key, String value)
        throws InvalidValueException, InvalidKeyException, DatabaseException
    {
        return stltConf.setProp(key, value);
    }

    @Override
    public @Nullable String removeCtrlProp(String key, String namespace)
        throws InvalidKeyException, DatabaseException
    {
        return ctrlConf.removeProp(key, namespace);
    }

    @Override
    public @Nullable String removeStltProp(String key, String namespace)
        throws InvalidKeyException, DatabaseException
    {
        return stltConf.removeProp(key, namespace);
    }

    @Override
    public ReadOnlyProps getCtrlConfForView()
    {
        return new ReadOnlyPropsImpl(ctrlConf);
    }

    @Override
    public Props getCtrlConfForChange()
    {
        return ctrlConf;
    }

    @Override
    public ReadOnlyProps getStltConfForView()
    {
        return new ReadOnlyPropsImpl(stltConf);
    }

}
