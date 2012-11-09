package ar.edu.itba.pod.legajo51190.impl;

import org.jgroups.Address;

/**
 * The answer of a node to tell it has received all the data from a sync node.
 * 
 * @author cris
 */
public class GlobalSyncNodeMessageAnswer extends NodeMessage {
	private static final long serialVersionUID = -7386708674429051203L;
	private final Address owner;

	public GlobalSyncNodeMessageAnswer(final Address owner) {
		this.owner = owner;
	}

	/**
	 * Node that sends the ACK
	 */
	public Address getOwner() {
		return owner;
	}
}
