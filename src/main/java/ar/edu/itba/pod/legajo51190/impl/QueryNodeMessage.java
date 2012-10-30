package ar.edu.itba.pod.legajo51190.impl;

import ar.edu.itba.pod.api.Signal;

public class QueryNodeMessage extends NodeMessage {
	private final Signal signal;
	private final int queryId;

	private static final long serialVersionUID = -3773685900519418431L;

	public QueryNodeMessage(final Signal signal, final int queryId) {
		this.signal = signal;
		this.queryId = queryId;
	}

	public Signal getSignal() {
		return signal;
	}

	public int getQueryId() {
		return queryId;
	}
}
