package com.linbit.linstor.layer.storage;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.layer.storage.diskless.DisklessProvider;
import com.linbit.linstor.layer.storage.ebs.EbsInitiatorProvider;
import com.linbit.linstor.layer.storage.ebs.EbsTargetProvider;
import com.linbit.linstor.layer.storage.file.FileProvider;
import com.linbit.linstor.layer.storage.file.FileThinProvider;
import com.linbit.linstor.layer.storage.lvm.LvmProvider;
import com.linbit.linstor.layer.storage.lvm.LvmThinProvider;
import com.linbit.linstor.layer.storage.spdk.SpdkLocalProvider;
import com.linbit.linstor.layer.storage.spdk.SpdkRemoteProvider;
import com.linbit.linstor.layer.storage.storagespaces.StorageSpacesProvider;
import com.linbit.linstor.layer.storage.storagespaces.StorageSpacesThinProvider;
import com.linbit.linstor.layer.storage.zfs.ZfsProvider;
import com.linbit.linstor.layer.storage.zfs.ZfsThinProvider;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import jakarta.inject.Provider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;

public class DeviceProviderModule extends AbstractModule
{
    private static final Map<DeviceProviderKind, Class<? extends DeviceProvider>> ALL_PROVIDERS;
    private final Node.Type nodeType;

    static
    {
        Map<DeviceProviderKind, Class<? extends DeviceProvider>> map = new HashMap<>();
        map.put(DeviceProviderKind.DISKLESS, DisklessProvider.class);
        map.put(DeviceProviderKind.EBS_INIT, EbsInitiatorProvider.class);
        map.put(DeviceProviderKind.EBS_TARGET, EbsTargetProvider.class);
        map.put(DeviceProviderKind.FILE, FileProvider.class);
        map.put(DeviceProviderKind.FILE_THIN, FileThinProvider.class);
        map.put(DeviceProviderKind.LVM, LvmProvider.class);
        map.put(DeviceProviderKind.LVM_THIN, LvmThinProvider.class);
        map.put(DeviceProviderKind.REMOTE_SPDK, SpdkRemoteProvider.class);
        map.put(DeviceProviderKind.SPDK, SpdkLocalProvider.class);
        map.put(DeviceProviderKind.STORAGE_SPACES, StorageSpacesProvider.class);
        map.put(DeviceProviderKind.STORAGE_SPACES_THIN, StorageSpacesThinProvider.class);
        map.put(DeviceProviderKind.ZFS, ZfsProvider.class);
        map.put(DeviceProviderKind.ZFS_THIN, ZfsThinProvider.class);
        ALL_PROVIDERS = Collections.unmodifiableMap(map);
    }

    public DeviceProviderModule(Node.Type nodeTypeRef)
    {
        nodeType = nodeTypeRef;
    }

    @Override
    protected void configure()
    {
        MapBinder<DeviceProviderKind, DeviceProvider> devProvidersBinder = MapBinder.newMapBinder(
            binder(),
            DeviceProviderKind.class,
            DeviceProvider.class
        );

        for (Map.Entry<DeviceProviderKind, Class<? extends DeviceProvider>> entry : ALL_PROVIDERS.entrySet())
        {
            DeviceProviderKind devProviderKind = entry.getKey();
            Class<? extends DeviceProvider> devProviderClass = entry.getValue();
            if (nodeType.isDeviceProviderKindAllowed(devProviderKind))
            {
                bind(devProvidersBinder, devProviderKind, devProviderClass);
            }
            else
            {
                // poison non-allowed providers
                poison(devProviderClass);
            }
        }
    }

    /**
     * <p>Explicit binding of a given class takes precedence to a simple {@code @Inject}-able version. This allows us
     * to use a custom DI-provider of {@link DeviceProvider} implementation that simply throws during guice's
     * inject-time if a class for example tries to directly inject into its constructor for example {@link LvmProvider}
     * while the current {@link Node.Type} would not allow that</p>
     *
     * <p>This method needs to be a dedicated method to convince compilers that both usages of DEV_PROV are the same
     * generic type instead of possibly two different interpretation of the "? extends..." wildcard.</p>
     */
    private <DEV_PROV extends DeviceProvider> void poison(Class<DEV_PROV> devProviderClassRef)
    {
        bind(devProviderClassRef).toProvider(new ForbiddenDevProvider<>(devProviderClassRef));
    }

    /**
     * <p>Binds the given {@code devProviderKindRef} to the given {@code devProviderClassRef}.</p>
     *
     * <p>This method needs to be a dedicated method to convince compilers that both usages of DEV_PROV are the same
     * generic type instead of possibly two different interpretation of the "? extends..." wildcard.</p>
     */
    private <DEV_PROV extends DeviceProvider> void bind(
        MapBinder<DeviceProviderKind, DeviceProvider> devProvidersBinderRef,
        DeviceProviderKind devProviderKindRef,
        Class<DEV_PROV> devProviderClassRef
    )
    {
        devProvidersBinderRef.addBinding(devProviderKindRef).to(devProviderClassRef);
    }

    /**
     * <p>Poisonous {@link Provider} that always throw when the specific class is requested.</p>
     *
     * <p>Used to prohibit explicit injection of {@link DeviceProvider}s that the local {@link Node.Type} does
     * not allow.</p>
     *
     * <p>This means that if your class needs to call a specific {@link DeviceProvider} directly, directly injecting
     * that class will cause an {@link ImplementationError} during dependency-injection. To prevent that, instead of
     * direct injection, inject {@link DeviceProviderMapper} and use its
     * {@link DeviceProviderMapper#getDeviceProviderByKindOrNull(DeviceProviderKind)} instead.</p>
     */
    class ForbiddenDevProvider<DEV_PROV extends DeviceProvider> implements Provider<DEV_PROV>
    {
        private final Class<DEV_PROV> devProviderClass;

        ForbiddenDevProvider(Class<DEV_PROV> devProviderClassRef)
        {
            devProviderClass = devProviderClassRef;
        }

        @Override
        public DEV_PROV get()
        {
            throw new ImplementationError(
                devProviderClass.getSimpleName() + " must not be injected on a " + nodeType.name() + " satellite"
            );
        }
    }
}
