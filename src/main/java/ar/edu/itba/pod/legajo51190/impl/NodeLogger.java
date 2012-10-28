package ar.edu.itba.pod.legajo51190.impl;

import java.io.PrintStream;

public class NodeLogger {
	private final Node node;
	private final PrintStream stream;
	private boolean enabled = true;

	public NodeLogger(final Node node) {
		this(node, System.out);
	}

	public NodeLogger(final Node node, final PrintStream stream) {
		this.stream = stream;
		this.node = node;
	}

	public void log(final String s) {
		if (isEnabled()) {
			stream.println("[" + node.getAddress() + "]:" + s);
		}
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(final boolean enabled) {
		this.enabled = enabled;
	}
}
