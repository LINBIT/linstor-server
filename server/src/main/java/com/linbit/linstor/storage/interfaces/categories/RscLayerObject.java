package com.linbit.linstor.storage.interfaces.categories;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.interfaces.RscLayerDataPojo;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface RscLayerObject extends LayerObject
{
    int getRscLayerId();

    @Nullable RscLayerObject getParent();

    void setParent(RscLayerObject parentRscLayerObject) throws SQLException;

    Set<RscLayerObject> getChildren();

    Resource getResource();

    String getResourceNameSuffix();

    @Nullable RscDfnLayerObject getRscDfnLayerObject();

    Map<VolumeNumber, ? extends VlmProviderObject> getVlmLayerObjects();

    default RscLayerObject getSingleChild()
    {
        Set<RscLayerObject> children = getChildren();
        if (children.size() != 1)
        {
            throw new ImplementationError("Exactly one child layer data expected but found: " +
                children.size() + " " + this.getClass().getSimpleName());
        }
        return children.iterator().next();
    }

    default ResourceName getResourceName()
    {
        return getResource().getDefinition().getName();
    }

    default String getSuffixedResourceName()
    {
        return getResourceName().displayValue + getResourceNameSuffix();
    }

    default boolean isFailed()
    {
        return getVlmLayerObjects().values().stream().anyMatch(VlmProviderObject::isFailed);
    }

    default Stream<? extends VlmProviderObject> streamVlmLayerObjects()
    {
        return getVlmLayerObjects().values().stream();
    }

    default VlmProviderObject getVlmProviderObject(VolumeNumber volumeNumber)
    {
        return getVlmLayerObjects().get(volumeNumber);
    }

    RscLayerDataPojo asPojo(AccessContext accCtx) throws AccessDeniedException;

    void delete();

    void remove(VolumeNumber vlmNrRef) throws SQLException;
}
