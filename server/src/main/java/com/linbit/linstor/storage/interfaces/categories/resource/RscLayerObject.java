package com.linbit.linstor.storage.interfaces.categories.resource;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.LayerObject;

import javax.annotation.Nullable;
import java.util.Iterator;
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

    void setParent(RscLayerObject parentRscLayerObject) throws DatabaseException;

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
        return getFirstChild();
    }

    default RscLayerObject getFirstChild()
    {
        Iterator<RscLayerObject> iterator = getChildren().iterator();
        RscLayerObject ret = null;
        if (iterator.hasNext())
        {
            ret = iterator.next();
        }
        return ret;
    }

    default RscLayerObject getChildBySuffix(String suffixRef)
    {
        Iterator<RscLayerObject> iterator = getChildren().iterator();
        RscLayerObject ret = null;
        String suffix = getResourceNameSuffix() + suffixRef;
        while (iterator.hasNext())
        {
            RscLayerObject rscObj = iterator.next();
            if (rscObj.getResourceNameSuffix().equals(suffix))
            {
                ret = rscObj;
                break;
            }
        }
        return ret;
    }

    default ResourceName getResourceName()
    {
        return getResource().getDefinition().getName();
    }

    default String getSuffixedResourceName()
    {
        return getResourceName().displayValue + getResourceNameSuffix();
    }

    default boolean hasFailed()
    {
        return getVlmLayerObjects().values().stream().anyMatch(VlmProviderObject::hasFailed);
    }

    default Stream<? extends VlmProviderObject> streamVlmLayerObjects()
    {
        return getVlmLayerObjects().values().stream();
    }

    default VlmProviderObject getVlmProviderObject(VolumeNumber volumeNumber)
    {
        return getVlmLayerObjects().get(volumeNumber);
    }

    RscLayerDataApi asPojo(AccessContext accCtx) throws AccessDeniedException;

    void delete() throws DatabaseException;

    void remove(AccessContext accCtx, VolumeNumber vlmNrRef) throws AccessDeniedException, DatabaseException;

    boolean checkFileSystem();

    void disableCheckFileSystem();
}
