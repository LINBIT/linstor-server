package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.AutoplaceStrategy.MinMax;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.AutoplaceStrategy.RatingAdditionalInfo;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer.StorPoolWithScore;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.strategies.MaximumFreeSpaceStrategy;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.strategies.MinimumReservedSpaceStrategy;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.strategies.MinimumResourceCountStrategy;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@Singleton
class StrategyHandler
{
    private final List<AutoplaceStrategy> strategies;
    private final SystemConfRepository sysCfgRep;
    private final AccessContext apiCtx;
    private final ErrorReporter errorReporter;

    private final Map<AutoplaceStrategy, Double> dfltWeights;

    @Inject
    StrategyHandler(
        SystemConfRepository sysCfgRepRef,
        @SystemContext AccessContext apiCtxRef,
        MaximumFreeSpaceStrategy freeSpaceStratRef,
        MinimumReservedSpaceStrategy minReservedSpaceStratRef,
        MinimumResourceCountStrategy minRscCountStratRef,
        ErrorReporter errorReporterRef
    )
    {
        strategies = Arrays.asList(freeSpaceStratRef, minReservedSpaceStratRef, minRscCountStratRef);
        sysCfgRep = sysCfgRepRef;
        apiCtx = apiCtxRef;
        errorReporter = errorReporterRef;

        dfltWeights = new HashMap<>();
        for (AutoplaceStrategy strat : strategies)
        {
            double dfltWeight = strat.getDefaultWeight();
            if (strat.getMinMax() == MinMax.MINIMIZE)
            {
                // The user should still be able to set a positive weight, even if the strategy is minimizing this
                // value. In that case, every time the property is parsed, it will be multiplied with -1.0 in the
                // end. Storing -DFLT_WEIGHT here simply allows us to skip this unnecessary multiplication when
                // using default values
                dfltWeight *= -1.0;
            }
            dfltWeights.put(strat, dfltWeight);
        }
    }

    public Collection<StorPoolWithScore> rate(
        Collection<StorPool> storPoolListRef
    )
        throws AccessDeniedException
    {
        RatingAdditionalInfo additionalInfo = new RatingAdditionalInfo();

        Map<AutoplaceStrategy, Double> strategyWeights = getWeights();

        Map<StorPool, StorPoolWithScore> lut = new HashMap<>();
        for (AutoplaceStrategy strat : strategies)
        {
            String stratName = strat.getName();
            double weight = strategyWeights.get(strat);

            Map<StorPool, Double> stratRate = strat.rate(storPoolListRef, additionalInfo);

            double highestValue = Double.NEGATIVE_INFINITY;
            for (Double stratValue : stratRate.values())
            {
                if (highestValue < stratValue && stratValue != 0.0)
                {
                    highestValue = stratValue;
                }
            }

            if (!stratRate.isEmpty())
            {
                errorReporter.logTrace(
                    "Autoplacer.Strategy: Scores of strategy '%s', weight: %f: " +
                        "(raw score, normalized score, weighted final score)",
                    stratName,
                    weight
                );
            }
            for (Entry<StorPool, Double> rate : stratRate.entrySet())
            {
                StorPool sp = rate.getKey();
                StorPoolWithScore prevRating = lut.get(sp);
                double stratValue = rate.getValue();

                if (prevRating == null)
                {
                    prevRating = new StorPoolWithScore(sp, 0);
                    lut.put(sp, prevRating);
                }
                // normalize and weight the value
                double normalizedVal;
                if (highestValue != Double.NEGATIVE_INFINITY)
                {
                    normalizedVal = stratValue / highestValue;
                }
                else
                {
                    normalizedVal = stratValue;
                }
                double normalizdWeightedVal = normalizedVal * weight;
                prevRating.score += normalizdWeightedVal;
                errorReporter.logTrace(
                    "Autoplacer.Strategy: Updated score of StorPool '%s' on Node '%s' to %f (%f, %f, %f)",
                    sp.getName().displayValue,
                    sp.getNode().getName().displayValue,
                    prevRating.score,
                    stratValue,
                    normalizedVal,
                    normalizdWeightedVal
                );
            }
        }

        return lut.values();
    }

    private Map<AutoplaceStrategy, Double> getWeights() throws AccessDeniedException
    {
        Map<AutoplaceStrategy, Double> weights = new HashMap<>(dfltWeights);

        ReadOnlyProps ctrlProps = sysCfgRep.getCtrlConfForView(apiCtx);
        @Nullable ReadOnlyProps weightsNs = ctrlProps.getNamespace(ApiConsts.NAMESPC_AUTOPLACER_WEIGHTS);
        if (weightsNs != null)
        {
            for (AutoplaceStrategy strat : strategies)
            {
                String stratName = strat.getName();
                String valStr = weightsNs.getProp(stratName);
                double valDouble;
                if (valStr != null)
                {
                    try
                    {
                        valDouble = Double.parseDouble(valStr);
                        errorReporter.logTrace(
                            "Autoplacer.Strategy: Strategy '%s' with weight: %f",
                            stratName,
                            valDouble
                        );
                    }
                    catch (NumberFormatException nfExc)
                    {
                        errorReporter.reportError(
                            nfExc,
                            null,
                            null,
                            "Could not parse '" + valStr + "' for strategy '" + stratName +
                                "'. Defaulting to 1.0"
                        );
                        valDouble = 1.0;
                    }

                    if (strat.getMinMax() == MinMax.MINIMIZE)
                    {
                        valDouble *= -1;
                    }

                    weights.put(strat, valDouble);
                }
            }
        }
        return weights;
    }
}
