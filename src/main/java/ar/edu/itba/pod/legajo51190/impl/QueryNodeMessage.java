package ar.edu.itba.pod.legajo51190.impl;

import java.util.Set;

import org.jgroups.Address;

import ar.edu.itba.pod.api.Signal;

public class QueryNodeMessage extends NodeMessage {
	private final Signal signal;
	private final int queryId;

	private static final long serialVersionUID = -3773685900519418431L;
	private final Set<Address> receiptMembers;

	public QueryNodeMessage(final Signal signal, final int queryId,
			final Set<Address> allButMe) {
		this.signal = signal;
		this.queryId = queryId;
		this.receiptMembers = allButMe;
	}

	public Signal getSignal() {
		return signal;
	}

	public int getQueryId() {
		return queryId;
	}

	public Set<Address> getReceiptMembers() {
		return receiptMembers;
	}
}
