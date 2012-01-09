/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.PartitionProperty;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The Optimizer representation of a <i>Reduce</i> contract node.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ReduceNode extends SingleInputNode {

	private List<ReduceNode> cachedPlans; // a cache for the computed alternative plans

	private float combinerReducingFactor = 1.0f; // the factor by which the combiner reduces the data

	/**
	 * Creates a new ReduceNode for the given contract.
	 * 
	 * @param pactContract
	 *        The reduce contract object.
	 */
	public ReduceNode(ReduceContract pactContract) {
		super(pactContract);
		
		// see if an internal hint dictates the strategy to use
		Configuration conf = getPactContract().getParameters();
		String localStrategy = conf.getString(PactCompiler.HINT_LOCAL_STRATEGY, null);

		if (localStrategy != null) {
			if (PactCompiler.HINT_LOCAL_STRATEGY_SORT.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.SORT);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_COMBINING_SORT.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.COMBININGSORT);
			} else {
				throw new CompilerException("Invalid local strategy hint for match contract: " + localStrategy);
			}
		} else {
			setLocalStrategy(LocalStrategy.NONE);
		}
	}

	/**
	 * Copy constructor to create a copy of a ReduceNode with a different predecessor. The predecessor
	 * is assumed to be of the same type and merely a copy with different strategies, as they
	 * are created in the process of the plan enumeration.
	 * 
	 * @param template
	 *        The ReduceNode to create a copy of.
	 * @param pred
	 *        The new predecessor.
	 * @param conn
	 *        The old connection to copy properties from.
	 * @param globalProps
	 *        The global properties of this copy.
	 * @param localProps
	 *        The local properties of this copy.
	 */
	protected ReduceNode(ReduceNode template, OptimizerNode pred, PactConnection conn, GlobalProperties globalProps,
			LocalProperties localProps) {
		super(template, pred, conn, globalProps, localProps);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this reduce node.
	 * 
	 * @return The contract.
	 */
	public ReduceContract getPactContract() {
		return (ReduceContract) super.getPactContract();
	}

	/**
	 * Checks, whether a combiner function has been given for the function encapsulated
	 * by this reduce contract.
	 * 
	 * @return True, if a combiner has been given, false otherwise.
	 */
	public boolean isCombineable() {
		return getPactContract().isCombinable();
	}

	/**
	 * Provides the optimizers decision whether an external combiner should be used or not.
	 * Current implementation is based on heuristics!
	 * 
	 * @return True, if an external combiner should be used, False otherwise
	 */
	public boolean useExternalCombiner() {
		if (!isCombineable()) {
			return false;
		} else {
			if (this.getInputConnection().getShipStrategy() == ShipStrategy.PARTITION_HASH
				|| this.getInputConnection().getShipStrategy() == ShipStrategy.PARTITION_RANGE) {
				return true;
			} else {
				return false;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Reduce";
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public int getMemoryConsumerCount() {
		switch(this.localStrategy) {
			case SORT:          return 1;
			case COMBININGSORT: return 1;
			case NONE:          return 0;
			default:	        return 0;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		super.setInputs(contractToNode);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingProperties()
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// check, if there is an output contract that tells us that certain properties are preserved.
		// if so, propagate to the child.
		List<InterestingProperties> thisNodesIntProps = getInterestingProperties();
		List<InterestingProperties> props = InterestingProperties.filterByKeepSet(thisNodesIntProps,
			getKeepSet(0));

		FieldSet keyFields = new FieldSet(getPactContract().getKeyColumnNumbers(0));
		
		// add the first interesting properties: partitioned and grouped
		InterestingProperties ip1 = new InterestingProperties();
		ip1.getGlobalProperties().setPartitioning(PartitionProperty.ANY, keyFields);
		ip1.getLocalProperties().setGrouped(true, keyFields);
		estimator.getHashPartitioningCost(this.input, ip1.getMaximalCosts());
		Costs c = new Costs();
		estimator.getLocalSortCost(this, this.input, c);
		ip1.getMaximalCosts().addCosts(c);

		// add the second interesting properties: partitioned only
		InterestingProperties ip2 = new InterestingProperties();
		ip2.getGlobalProperties().setPartitioning(PartitionProperty.ANY, keyFields);
		estimator.getHashPartitioningCost(this.input, ip2.getMaximalCosts());

		InterestingProperties.mergeUnionOfInterestingProperties(props, ip1);
		InterestingProperties.mergeUnionOfInterestingProperties(props, ip2);

		input.addAllInterestingProperties(props);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans(eu.stratosphere.pact
	 * .compiler.costs.CostEstimator)
	 */
	@Override
	public List<ReduceNode> getAlternativePlans(CostEstimator estimator) {
		// check if we have a cached version
		if (cachedPlans != null) {
			return cachedPlans;
		}

		List<? extends OptimizerNode> inPlans = input.getSourcePact().getAlternativePlans(estimator);
		List<ReduceNode> outputPlans = new ArrayList<ReduceNode>();

		// reduce has currently only one strategy: if the data is not already partitioned, partition it by
		// hash, sort it locally

		for (OptimizerNode pred : inPlans) {
			ShipStrategy ss = input.getShipStrategy();
			// ShipStrategy ss2 = null;

			LocalStrategy ls = getLocalStrategy();

			GlobalProperties gp;
			LocalProperties lp;

			if (ss == ShipStrategy.NONE) {
				gp = pred.getGlobalProperties();
				lp = pred.getLocalProperties();

				if (gp.getPartitioning().isPartitioned()) { //|| gp.isKeyUnique()) {
					ss = ShipStrategy.FORWARD;
				} else {
					ss = ShipStrategy.PARTITION_HASH;
					// ss2 = ShipStrategy.PARTITION_RANGE;
				}

				gp = PactConnection.getGlobalPropertiesAfterConnection(pred, this, ss);
				lp = PactConnection.getLocalPropertiesAfterConnection(pred, this, ss);
			} else {
				// fixed strategy
				gp = PactConnection.getGlobalPropertiesAfterConnection(pred, this, ss);
				lp = PactConnection.getLocalPropertiesAfterConnection(pred, this, ss);

//				if (!(gp.getPartitioning().isPartitioned() || gp.isKeyUnique())) {
				if (!(gp.getPartitioning().isPartitioned())) {
					// the shipping strategy is fixed to a value that does not leave us with
					// the necessary properties. this candidate cannot produce a valid child
					continue;
				}
			}
			
			FieldSet keySet = new FieldSet(getPactContract().getKeyColumnNumbers(0));
			boolean localStrategyNeeded = !lp.getOrdering().groupsFieldSet(keySet);

			if (localStrategyNeeded && lp.isGrouped() == true) {
				localStrategyNeeded = !lp.getGroupedFields().containsAll(keySet);
			}
			

			// see, whether we need a local strategy
//			if (!(lp.areKeysGrouped() || lp.getKeyOrder().isOrdered() || lp.isKeyUnique())) {
			if (localStrategyNeeded) {
			
				// we need one
				if (ls != LocalStrategy.NONE) {
					if (ls != LocalStrategy.COMBININGSORT && ls != LocalStrategy.SORT) {
						// no valid plan possible
						continue;
					}
				}
				// local strategy free to choose
				else {
					ls = isCombineable() ? LocalStrategy.COMBININGSORT : LocalStrategy.SORT;
				}
			}

			// adapt the local properties
			if (ls == LocalStrategy.COMBININGSORT || ls == LocalStrategy.SORT) {
				Ordering ordering = new Ordering();
				for (Integer index :keySet) {
					ordering.appendOrdering(index, Order.ASCENDING);
				}
				lp.setOrdering(ordering);
				lp.setGrouped(true, keySet);
			}

			// ----------------------------------------------------------------
			// see, if we have a combiner before shipping
			if (isCombineable() && ss != ShipStrategy.FORWARD) {
				// this node contains the estimates for the costs of the combiner,
				// as well as the updated size and cardinality estimates
				OptimizerNode combiner = new CombinerNode(getPactContract(), pred, combinerReducingFactor);
				combiner.setDegreeOfParallelism(pred.getDegreeOfParallelism());

				estimator.costOperator(combiner);
				pred = combiner;
			}
			// ----------------------------------------------------------------

			// create a new reduce node for this input
			ReduceNode n = new ReduceNode(this, pred, input, gp, lp);
			n.input.setShipStrategy(ss);
			n.setLocalStrategy(ls);

			// compute, which of the properties survive, depending on the output contract
			n.getGlobalProperties().filterByKeepSet(getKeepSet(0));
			n.getLocalProperties().filterByKeepSet(getKeepSet(0));

			estimator.costOperator(n);

			outputPlans.add(n);

			// see, if we also have another partitioning alternative
			// if (ss2 != null) {
			// gp = PactConnection.getGlobalPropertiesAfterConnection(pred, ss2);
			// lp = PactConnection.getLocalPropertiesAfterConnection(pred, ss2);
			//				
			// // see, if we need a local strategy
			// if (!(lp.getKeyOrder().isOrdered() || lp.isKeyUnique())) {
			// lp.setKeyOrder(Order.ASCENDING);
			// ls = isCombineable() ? LocalStrategy.COMBININGSORT : LocalStrategy.SORT;
			// }
			//				
			// // create a new reduce node for this input
			// n = new ReduceNode(this, pred, input, gp, lp);
			// n.input.setShipStrategy(ss2);
			// n.setLocalStrategy(ls);
			//				
			// // compute, which of the properties survive, depending on the output contract
			// n.getGlobalProperties().getPreservedAfterContract(getOutputContract());
			// n.getLocalProperties().getPreservedAfterContract(getOutputContract());
			//				
			// // compute the costs
			// estimator.costOperator(n);
			//				
			// outputPlans.add(n);
			// }
		}

		// check if the list does not contain any plan. That may happen, if the channels specify
		// incompatible shipping strategies.
		if (outputPlans.isEmpty()) {
			throw new CompilerException("Could not create a valid plan for the reduce contract '"
				+ getPactContract().getName() + "'. The compiler hints specified incompatible shipping strategies.");
		}

		// prune the plans
		prunePlanAlternatives(outputPlans);

		// cache the result only if we have multiple outputs --> this function gets invoked multiple times
		if (this.getOutgoingConnections() != null && this.getOutgoingConnections().size() > 1) {
			this.cachedPlans = outputPlans;
		}

		return outputPlans;
	}
	
	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	private long computeNumberOfProcessedKeys() {
		OptimizerNode pred = input == null ? null : input.getSourcePact();

		if(pred != null) {
			// Each key is processed by Reduce
			FieldSet columnSet = new FieldSet(getPactContract().getKeyColumnNumbers(0));
			return pred.getEstimatedCardinality(columnSet);
		} else {
			return -1;
		}
	}
	
	/**
	 * Computes the number of stub calls for one processed key. 
	 * 
	 * @return the number of stub calls for one processed key.
	 */
//	private double computeStubCallsPerProcessedKey() {
//		
//		// the stub is called once for each key.
//		return 1;
//	}
	
	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	protected long computeNumberOfStubCalls() {
		
		// the stub is called once per key
		return this.computeNumberOfProcessedKeys();
	}
	
	/**
	 * Computes the width of output records
	 * 
	 * @return width of output records
	 */
	protected double computeAverageRecordWidth() {
		OptimizerNode pred = input == null ? null : input.getSourcePact();
		CompilerHints hints = getPactContract().getCompilerHints();
		
		if(hints.getAvgBytesPerRecord() != -1) {
			// use hint if available
			return hints.getAvgBytesPerRecord();
		
		} else if (pred != null) {
			// use record width of previous node
			
			if(pred.estimatedOutputSize != -1 && pred.estimatedNumRecords != -1) {
				return (pred.getEstimatedOutputSize() / pred.getEstimatedNumRecords()) >= 1 ? 
						(long) (pred.getEstimatedOutputSize() / pred.getEstimatedNumRecords()) : 1;
			} else {
				return -1.0;
			}
			
		} else {
			// we have no estimate for the width... 
			return -1.0;
		}
	}
	
	private void computeCombinerReducingFactor() {
		OptimizerNode pred = input == null ? null : input.getSourcePact();
		
		if (isCombineable() && pred.estimatedNumRecords >= 1 && computeNumberOfProcessedKeys() >= 1
			&& pred.estimatedOutputSize >= -1) {
			int parallelism = pred.getDegreeOfParallelism();
			parallelism = parallelism >= 1 ? parallelism : 32; // @parallelism

			float inValsPerKey = ((float) pred.estimatedNumRecords) / computeNumberOfProcessedKeys();
			float valsPerNode = inValsPerKey / parallelism;
			valsPerNode = valsPerNode >= 1.0f ? valsPerNode : 1.0f;

			this.combinerReducingFactor = 1.0f / valsPerNode;
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputEstimates(eu.stratosphere.pact.compiler.DataStatistics)
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		OptimizerNode pred = input == null ? null : input.getSourcePact();
//		CompilerHints hints = getPactContract().getCompilerHints();
		
		// special hint handling for Reduce:
		// In case of SameKey OutputContract, avgNumValuesPerKey and avgRecordsEmittedPerStubCall are identical, 
		// since the stub is called once per key
//		if(this.getOutputContract().equals(OutputContract.SameKey)) {
//			if(hints.getAvgNumValuesPerKey() != -1 && hints.getAvgRecordsEmittedPerStubCall() == -1) {
//				hints.setAvgRecordsEmittedPerStubCall(hints.getAvgNumValuesPerKey());
//			}
//			if(hints.getAvgRecordsEmittedPerStubCall() != -1 && hints.getAvgNumValuesPerKey() == -1) {
//				hints.setAvgNumValuesPerKey(hints.getAvgRecordsEmittedPerStubCall());
//			}
//		}
		super.computeOutputEstimates(statistics);
		// check if preceding node is available
		if (pred != null) {
			this.computeCombinerReducingFactor();
		}
	}

}
