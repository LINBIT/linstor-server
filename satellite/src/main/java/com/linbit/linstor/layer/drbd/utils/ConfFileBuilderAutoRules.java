package com.linbit.linstor.layer.drbd.utils;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;

import java.util.HashMap;

public class ConfFileBuilderAutoRules
{
    private final HashMap<String, AutoRule> autoRules = new HashMap<>();

    public ConfFileBuilderAutoRules(AccessContext accCtx, DrbdVlmData<Resource> drbdVlmData)
        throws AccessDeniedException
    {
        appendRules(accCtx, drbdVlmData.getRscLayerObject());
        appendRules(accCtx, drbdVlmData);
    }

    public ConfFileBuilderAutoRules(AccessContext accCtx, DrbdRscData<Resource> drbdRscData)
        throws AccessDeniedException
    {
        appendRules(accCtx, drbdRscData);
    }

    private void appendRules(AccessContext accCtxRef, DrbdRscData<Resource> drbdRscDataRef) throws AccessDeniedException
    {

    }

    private void appendRules(AccessContext accCtxRef, DrbdVlmData<Resource> drbdVlmDataRef) throws AccessDeniedException
    {
        autoRules.put(
            ApiConsts.NAMESPC_DRBD_DISK_OPTIONS + "/rs-discard-granularity",
            new AutoRule(
                ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_RS_DISCARD_GRANULARITY,
                drbdVlmDataRef.getVolume().getVolumeDefinition().getProps(accCtxRef),
                false
            )
        );
    }

    public AutoRule get(String keyWithNamespaceRef)
    {
        return autoRules.get(keyWithNamespaceRef);
    }

    public static class AutoRule
    {
        public final String key;
        public final ReadOnlyProps props;
        public final boolean isNullAllowed;

        private AutoRule(String keyRef, ReadOnlyProps propsRef, boolean isNullAllowedRef)
        {
            key = keyRef;
            props = propsRef;
            isNullAllowed = isNullAllowedRef;
        }
    }
}
