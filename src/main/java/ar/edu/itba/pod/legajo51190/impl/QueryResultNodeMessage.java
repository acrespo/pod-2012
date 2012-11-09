package ar.edu.itba.pod.legajo51190.impl;

import ar.edu.itba.pod.api.Result;

public class QueryResultNodeMessage extends NodeMessage {

	private static final long serialVersionUID = -3300889007076885804L;

	private final int messageId;

	private final Result result;

	public QueryResultNodeMessage(final int messageId, final Result result) {
		this.messageId = messageId;
		this.result = result;

	}

	public int getMessageId() {
		return messageId;
	}

	public Result getResult() {
		return result;
	}

}
