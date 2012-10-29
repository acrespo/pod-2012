package ar.edu.itba.pod.legajo51190.impl;

import java.io.PrintStream;

/**
 * Helper class for logging the actions of a node reporting the node name
 * 
 * @author cris
 */
public class NodeLogger {
	private final JGroupNode node;
	private final PrintStream stream;
	private boolean enabled = true;
	private StringBuilder builder = new StringBuilder();

	public NodeLogger(final JGroupNode node) {
		this(node, System.out);
	}

	public NodeLogger(final JGroupNode node, final PrintStream stream) {
		this.stream = stream;
		this.node = node;
	}

	/**
	 * Acumulative log, must be flushed to be shown.
	 */
	public synchronized void logAcum(final String s) {
		builder.append("[" + node.getAddress() + "]:" + s + "\n");
	}

	/**
	 * Flushes the log to output
	 */
	public synchronized void flush() {
		stream.print(builder.toString());
		builder = new StringBuilder();
	}

	/**
	 * Simply logs
	 */
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
