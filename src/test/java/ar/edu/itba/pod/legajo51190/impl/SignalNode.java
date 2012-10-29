package ar.edu.itba.pod.legajo51190.impl;

import ar.edu.itba.pod.api.SPNode;
import ar.edu.itba.pod.api.SignalProcessor;

/**
 * Interface I used to make easier tests.
 * 
 * @author cris
 */
public interface SignalNode extends SignalProcessor, SPNode {
	/**
	 * Listener used to synchronize tests with a set of nodes.
	 */
	SyncListener getInjectedListener();

	/**
	 * Node containing all the relevant information.
	 */
	JGroupNode getJGroupNode();
}
