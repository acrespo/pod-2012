package ar.edu.itba.pod.legajo51190.impl;

import org.jgroups.Address;

/**
 * Custom implementation of a SignalProcessor and SPNode, used for testing.
 * 
 * @author cris
 */
public interface JGroupSignalProcessor {

	/**
	 * The internal data representation.
	 */
	public JGroupNode getJGroupNode();

	/**
	 * Builds a new remote query with the message received
	 */
	public void onQueryReception(QueryNodeMessage query, Address from);

	/**
	 * Builds a new remote query with the message received
	 */
	public void onResultReception(QueryResultNodeMessage answer, Address from);

	/**
	 * Tells the signal processor that all the fallen nodes are fixed
	 */
	public void onNodeGoneFixed();
}
