package ar.edu.itba.pod.legajo51190.impl;

import java.util.List;
import java.util.Set;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Signal;

import com.google.common.collect.Multimap;

/**
 * This message contains the information for a new node to join the group. Each
 * member of the cluster must send this message, and must lock awaiting a
 * {@link GlobalSyncNodeMessageAnswer}
 * 
 * @author cris
 */
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

	/**
	 * Signals the destination nodes will receive
	 */
	public Multimap<Address, Signal> getSignalsMap() {
		return signalsMap;
	}

	/**
	 * Signals for backup for each new destination node
	 */
	public Multimap<Address, Signal> getBackupSignals() {
		return backupSignals;
	}

	/**
	 * Tells whether the sync is a copy sync, which is normally done only on the
	 * first time
	 */
	public boolean isCopyMode() {
		return copyMode;
	}

	/**
	 * Returns the nodes new nodes that are receptors of this message.
	 */
	public Set<Address> getDestinations() {
		return getSignalsMap().keySet();
	}

	/**
	 * All the members considered in this message, this is because the receptor
	 * might not yet be syncd with all this data, and it's neccesary to
	 * distribute the data
	 */
	public List<Address> getAllMembers() {
		return allMembers;
	}

}
