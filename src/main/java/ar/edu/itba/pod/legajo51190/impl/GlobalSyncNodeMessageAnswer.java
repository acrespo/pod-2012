package ar.edu.itba.pod.legajo51190.impl;

import org.jgroups.Address;

public class GlobalSyncNodeMessageAnswer extends NodeMessage {
	private static final long serialVersionUID = -7386708674429051203L;
	private final Address owner;

	public GlobalSyncNodeMessageAnswer(final Address owner) {
		this.owner = owner;

	}

	public Address getOwner() {
		return owner;
	}
}
