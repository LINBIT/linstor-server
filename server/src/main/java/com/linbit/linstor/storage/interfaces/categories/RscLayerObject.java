package com.linbit.linstor.storage.interfaces.categories;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeNumber;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface RscLayerObject extends LayerObject
{
    @Nullable RscLayerObject getParent();

    List<RscLayerObject> getChildren();

    Resource getResource();

    String getResourceNameSuffix();

    @Nullable RscDfnLayerObject getRscDfnLayerObject();

    Map<VolumeNumber, ? extends VlmProviderObject> getVlmLayerObjects();

    default RscLayerObject getSingleChild()
    {
        List<RscLayerObject> children = getChildren();
        if (children.size() != 1)
        {
            throw new ImplementationError("Exactly one child layer data expected but found: " +
                children.size() + " " + this.getClass().getSimpleName());
        }
        return children.get(0);
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
}
