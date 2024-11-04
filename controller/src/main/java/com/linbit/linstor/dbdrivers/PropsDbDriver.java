package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.PropsCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsDatabaseDriver.PropsDbEntry;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.PROPS_CONTAINERS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.PropsContainers.PROPS_INSTANCE;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.PropsContainers.PROP_KEY;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.PropsContainers.PROP_VALUE;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@Singleton
public class PropsDbDriver extends AbsProtectedDatabaseDriver<PropsDbEntry, Void, Void>
    implements PropsCtrlDatabaseDriver
{
    private final SingleColumnDatabaseDriver<PropsDbEntry, String> valueDriver;
    private final Map<String, Map<String, String>> cachedPropsConMap;
    private int cachedLoadedPropsCount = 0;

    @Inject
    public PropsDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        ObjectProtectionFactory objProtFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, PROPS_CONTAINERS, dbEngineRef, objProtFactoryRef);

        setColumnSetter(
            PROPS_INSTANCE,
            propDbEntry -> propDbEntry.propsInstance.toUpperCase()
        );
        setColumnSetter(PROP_KEY, propDbEntry -> propDbEntry.propKey);
        setColumnSetter(PROP_VALUE, propDbEntry -> propDbEntry.propValue);

        valueDriver = generateSingleColumnDriver(
            PROP_VALUE,
            propDbEntry -> propDbEntry.propValue,
            Function.identity()
        );
        cachedPropsConMap = new HashMap<>();
    }

    @Override
    public SingleColumnDatabaseDriver<PropsDbEntry, String> getValueDriver()
    {
        return valueDriver;
    }

    @Override
    protected @Nullable Pair<PropsDbEntry, Void> load(RawParameters rawRef, Void parentRef)
        throws DatabaseException
    {
        String instanceName = rawRef.get(PROPS_INSTANCE);
        String key = rawRef.get(PROP_KEY);
        String value = rawRef.get(PROP_VALUE);

        cachedPropsConMap
            .computeIfAbsent(instanceName.toUpperCase(), missingInstanceName -> new HashMap<>())
            .put(key, value);

        cachedLoadedPropsCount++;
        return null;
    }

    @Override
    public Map<String, String> loadCachedInstance(String propsInstance)
    {
        Map<String, String> map = cachedPropsConMap.get(propsInstance.toUpperCase());
        if (map == null)
        {
            map = Collections.emptyMap(); // to prevent null-checks on caller side
        }
        return map;
    }

    @Override
    protected int getLoadedCount(Map<PropsDbEntry, Void> ignoredEmptyMap)
    {
        // do not return cachedPropsConMap.size() here, since that only returns the instance count, not the actual prop
        // count
        return cachedLoadedPropsCount;
    }

    @Override
    public void clearCache()
    {
        cachedPropsConMap.clear();
        cachedLoadedPropsCount = 0;
    }

    @Override
    protected String getId(PropsDbEntry dataRef) throws AccessDeniedException
    {
        StringBuilder id = new StringBuilder("(InstanceName=").append(dataRef.propsInstance);
        id.append(" Key=").append(dataRef.propKey);
        id.append(" Value=").append(dataRef.propValue); // if value does not exist, it is "" instead of null
        id.append(")");
        return id.toString();
    }
}
