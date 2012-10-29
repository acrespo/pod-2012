package ar.edu.itba.pod.legajo51190.impl;

import ar.edu.itba.pod.api.SPNode;
import ar.edu.itba.pod.api.SignalProcessor;

/**
 * Custom implementation of a SignalProcessor and SPNode, used for testing.
 * 
 * @author cris
 */
public interface JGroupSignalProcessor extends SignalProcessor, SPNode {

	/**
	 * The internal data representation.
	 */
	public JGroupNode getJGroupNode();
}
