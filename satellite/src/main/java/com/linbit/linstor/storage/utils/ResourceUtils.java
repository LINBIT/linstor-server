package com.linbit.linstor.storage.utils;

import com.linbit.linstor.Resource;
import com.linbit.linstor.Volume;
import com.linbit.linstor.storage.VlmStorageState;
import com.linbit.utils.ExceptionThrowingBiConsumer;
import com.linbit.utils.ExceptionThrowingConsumer;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class ResourceUtils
{
    public static <EXC extends Exception> void foreachVlm(
        Collection<Resource> rscs,
        ExceptionThrowingConsumer<Volume, EXC> func
    )
        throws EXC
    {
        for (final Resource rsc : rscs)
        {
            final Iterator<Volume> vlmIt = rsc.iterateVolumes();
            while (vlmIt.hasNext())
            {
                func.accept(vlmIt.next());
            }
        }
    }

    public static <EXC extends Exception> void foreachVlm(
        Collection<Resource> rscs,
        Map<Volume, VlmStorageState> states,
        ExceptionThrowingBiConsumer<Volume, VlmStorageState, EXC> func
    )
        throws EXC
    {
        for (final Resource rsc : rscs)
        {
            final Iterator<Volume> vlmIt = rsc.iterateVolumes();
            while (vlmIt.hasNext())
            {
                Volume vlm = vlmIt.next();
                func.accept(vlm, states.get(vlm));
            }
        }
    }

}
