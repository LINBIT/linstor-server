package com.linbit.linstor.core.objects;


import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.ExceptionThrowingBiFunction;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AutoUnselectorConfig
{
    public static final String FILTER_DISKFUL = "diskful";
    public static final String FILTER_DISKLESS = "diskless";

    private final ResourceDefinition rscDfn;
    private final Map<Resource, AutoUnselectRscConfig> unplaceSettingMap;
    private final ExceptionThrowingBiFunction<
        AccessContext, Resource, /* @Nullable */ String, AccessDeniedException> filter;

    public AutoUnselectorConfig(
        ResourceDefinition rscDfnRef,
        Map<Resource, AutoUnselectRscConfig> unplaceSettingMapRef,
        ExceptionThrowingBiFunction<AccessContext, Resource, /* @Nullable */ String, AccessDeniedException> filterRef
    )
    {
        rscDfn = rscDfnRef;
        unplaceSettingMap = unplaceSettingMapRef;
        filter = filterRef;
    }

    public ResourceDefinition getRscDfn()
    {
        return rscDfn;
    }

    public ExceptionThrowingBiFunction<AccessContext, Resource, String, AccessDeniedException> getFilter()
    {
        return filter;
    }

    public AutoUnselectRscConfig getBy(Resource rscRef)
    {
        @Nullable AutoUnselectRscConfig ret = unplaceSettingMap.get(rscRef);
        if (ret == null)
        {
            ret = AutoUnselectRscConfig.DFLT_INSTANCE;
        }
        return ret;
    }

    public static class AutoUnselectRscConfig
    {
        public static final AutoUnselectRscConfig DFLT_INSTANCE = new AutoUnselectRscConfig(
            null
        );

        private final @Nullable String ignoreReason;

        public AutoUnselectRscConfig(@Nullable String ignoreReasonRef)
        {
            ignoreReason = ignoreReasonRef;
        }

        public @Nullable String getIgnoreReason()
        {
            return ignoreReason;
        }
    }

    public static class CfgBuilder
    {
        private final ResourceDefinition rscDfn;
        private final Map<Resource, RscConfigBuilder> rscCfgMap = new HashMap<>();

        private ExceptionThrowingBiFunction<AccessContext, Resource, /* @Nullable */ String, AccessDeniedException> filter;

        public CfgBuilder(ResourceDefinition rscDfnRef)
        {
            rscDfn = rscDfnRef;

            setDiskfulFilter();
        }

        public CfgBuilder setDiskfulFilter()
        {
            return setFilter((accCtx, rsc) -> rsc.isDiskless(accCtx) ? FILTER_DISKLESS : null);
        }

        public CfgBuilder setDisklessFilter()
        {
            return setFilter((accCtx, rsc) -> rsc.isDiskless(accCtx) ? null : FILTER_DISKFUL);
        }

        public CfgBuilder setFilter(
            ExceptionThrowingBiFunction<AccessContext, Resource, String, AccessDeniedException> filterRef
        )
        {
            filter = filterRef;
            return this;
        }

        public RscConfigBuilder forRsc(Resource rscRef)
        {
            return rscCfgMap.computeIfAbsent(rscRef, ignore -> new RscConfigBuilder());
        }

        public AutoUnselectorConfig build(AccessContext apiAccCtx) throws AccessDeniedException
        {
            applyFilter(apiAccCtx);

            Map<Resource, AutoUnselectRscConfig> autoUnselectorRscCfgMap = new HashMap<>();
            for (Map.Entry<Resource, RscConfigBuilder> entry : rscCfgMap.entrySet())
            {
                autoUnselectorRscCfgMap.put(entry.getKey(), entry.getValue().build());
            }
            return new AutoUnselectorConfig(rscDfn, autoUnselectorRscCfgMap, filter);
        }

        private void applyFilter(AccessContext apiAccCtx) throws AccessDeniedException
        {
            Iterator<Resource> rscIt = rscDfn.iterateResource(apiAccCtx);
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();

                RscConfigBuilder rscCfgBuilder = forRsc(rsc);
                @Nullable String alreadySetIgnoreReason = rscCfgBuilder.ignoreReason;
                if (alreadySetIgnoreReason == null)
                {
                    rscCfgBuilder.setIgnoreReason(filter.accept(apiAccCtx, rsc));
                }
            }
        }

        public CfgBuilder setFilterForFixedResources(Collection<Resource> fixedResourcesRef)
        {
            for (Resource fixedRsc : fixedResourcesRef)
            {
                forRsc(fixedRsc).setIgnoreReason("fixed resource");
            }
            return this;
        }
    }

    public static class RscConfigBuilder
    {
        private @Nullable String ignoreReason = null;

        public @Nullable String getIgnoreReason()
        {
            return ignoreReason;
        }

        public RscConfigBuilder setIgnoreReason(@Nullable String ignoreReasonRef)
        {
            ignoreReason = ignoreReasonRef;
            return this;
        }

        public AutoUnselectRscConfig build()
        {
            return new AutoUnselectRscConfig(ignoreReason);
        }
    }
}
