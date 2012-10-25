package ar.edu.itba.pod.legajo51190.impl;

import java.io.Serializable;

public final class NodeMessage implements Serializable {
	private static final long serialVersionUID = -3347347350882842763L;

	public static final int MESSAGE_NEW_NODE_SYNC = 1;
	public static final int MESSAGE_NEW_NODE_SYNC_ANSWER = 2;

	private final Serializable content;
	private final int type;

	public NodeMessage(final int type) {
		super();
		this.type = type;
		content = null;
	}

	public NodeMessage(final Serializable content, final int type) {
		super();
		this.content = content;
		this.type = type;
	}

	public Serializable getContent() {
		return content;
	}

	public int getType() {
		return type;
	}
}
