package ar.edu.itba.pod.legajo51190.impl;

/**
 * Helpful for creating custom events and testing them.
 * 
 * @author cris
 */
public interface NodeListener {

	/**
	 * Triggered after a new node has been successfully added and has told us so
	 * by himself
	 */
	public void onNodeSyncDone();

	/**
	 * Triggered after a node has left the group and all the other nodes have
	 * successfully synchronized, and all of them have told them so by
	 * themselves
	 */
	public void onNodeGoneSyncDone();
}
