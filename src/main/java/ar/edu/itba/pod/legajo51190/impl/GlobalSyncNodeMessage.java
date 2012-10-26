package ar.edu.itba.pod.legajo51190.impl;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Signal;

import com.google.common.collect.Multimap;

public class GlobalSyncNodeMessage extends NodeMessage {
	private static final long serialVersionUID = -7386708674429051203L;
	private final Multimap<Address, Signal> signalsMap;

	public GlobalSyncNodeMessage(final Multimap<Address, Signal> signalsMap) {
		super();
		this.signalsMap = signalsMap;
	}

	public Multimap<Address, Signal> getSignalsMap() {
		return signalsMap;
	}

}
