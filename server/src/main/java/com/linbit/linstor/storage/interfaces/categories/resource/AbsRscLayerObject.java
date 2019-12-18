package com.linbit.linstor.storage.interfaces.categories.resource;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
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
public interface AbsRscLayerObject<RSC extends AbsResource<RSC>>
    extends LayerObject
{
    int getRscLayerId();

    @Nullable
    AbsRscLayerObject<RSC> getParent();

    void setParent(AbsRscLayerObject<RSC> parentRscLayerObject) throws DatabaseException;

    Set<AbsRscLayerObject<RSC>> getChildren();

    RSC getAbsResource();

    String getResourceNameSuffix();

    @Nullable RscDfnLayerObject getRscDfnLayerObject();

    Map<VolumeNumber, ? extends VlmProviderObject<RSC>> getVlmLayerObjects();

    RscLayerDataApi asPojo(AccessContext accCtx) throws AccessDeniedException;

    void delete() throws DatabaseException;

    void remove(AccessContext accCtx, VolumeNumber vlmNrRef) throws AccessDeniedException, DatabaseException;

    boolean checkFileSystem();

    void disableCheckFileSystem();

    void setSuspendIo(boolean suspend) throws DatabaseException;

    boolean getSuspendIo();

    default AbsRscLayerObject<RSC> getSingleChild()
    {
        Set<AbsRscLayerObject<RSC>> children = getChildren();
        if (children.size() != 1)
        {
            throw new ImplementationError("Exactly one child layer data expected but found: " +
                children.size() + " " + this.getClass().getSimpleName());
        }
        return getFirstChild();
    }

    default AbsRscLayerObject<RSC> getFirstChild()
    {
        Iterator<AbsRscLayerObject<RSC>> iterator = getChildren().iterator();
        AbsRscLayerObject<RSC> ret = null;
        if (iterator.hasNext())
        {
            ret = iterator.next();
        }
        return ret;
    }

    default AbsRscLayerObject<RSC> getChildBySuffix(String suffixRef)
    {
        Iterator<AbsRscLayerObject<RSC>> iterator = getChildren().iterator();
        AbsRscLayerObject<RSC> ret = null;
        String suffix = getResourceNameSuffix() + suffixRef;
        while (iterator.hasNext())
        {
            AbsRscLayerObject<RSC> rscObj = iterator.next();
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
        return getAbsResource().getResourceDefinition().getName();
    }

    default String getSuffixedResourceName()
    {
        return getResourceName().displayValue + getResourceNameSuffix();
    }

    default boolean hasFailed()
    {
        return getVlmLayerObjects().values().stream().anyMatch(VlmProviderObject::hasFailed);
    }

    default Stream<? extends VlmProviderObject<RSC>> streamVlmLayerObjects()
    {
        return getVlmLayerObjects().values().stream();
    }

    default <T extends VlmProviderObject<RSC>> T getVlmProviderObject(VolumeNumber volumeNumber)
    {
        return (T) getVlmLayerObjects().get(volumeNumber);
    }
}
