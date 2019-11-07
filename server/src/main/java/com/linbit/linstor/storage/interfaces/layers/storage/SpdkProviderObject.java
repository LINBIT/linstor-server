package com.linbit.linstor.storage.interfaces.layers.storage;

import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

public interface SpdkProviderObject extends VlmProviderObject
{
    enum SpdkFlags implements Flags
    {
        EXISTS(1L << 0),
        FAILED(1L << 1);

        private long flagValue;

        SpdkFlags(long flagValueRef)
        {
            flagValue = flagValueRef;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }
    }
}
