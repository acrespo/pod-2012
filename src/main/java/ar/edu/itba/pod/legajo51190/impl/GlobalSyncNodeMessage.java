package ar.edu.itba.pod.legajo51190.impl;

import java.util.List;
import java.util.Set;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Signal;

import com.google.common.collect.Multimap;

public class GlobalSyncNodeMessage extends NodeMessage {
	private static final long serialVersionUID = -7386708674429051203L;
	private final Multimap<Address, Signal> signalsMap;
	private final Multimap<Address, Signal> backupSignals;
	private final boolean copyMode;
	private final List<Address> allMembers;

	public GlobalSyncNodeMessage(final Multimap<Address, Signal> signalsMap,
			final Multimap<Address, Signal> backupSignals,
			final boolean copyMode, final List<Address> allMembers) {
		super();
		this.signalsMap = signalsMap;
		this.backupSignals = backupSignals;
		this.copyMode = copyMode;
		this.allMembers = allMembers;
	}

	public Multimap<Address, Signal> getSignalsMap() {
		return signalsMap;
	}

	public Multimap<Address, Signal> getBackupSignals() {
		return backupSignals;
	}

	public boolean isCopyMode() {
		return copyMode;
	}

	public Set<Address> getDestinations() {
		return getSignalsMap().keySet();
	}

	public List<Address> getAllMembers() {
		return allMembers;
	}

}
