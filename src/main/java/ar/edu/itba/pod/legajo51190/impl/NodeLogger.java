package ar.edu.itba.pod.legajo51190.impl;

import java.io.PrintStream;

public class NodeLogger {
	private final Node node;
	private final PrintStream stream;

	public NodeLogger(final Node node) {
		this(node, System.out);
	}

	public NodeLogger(final Node node, final PrintStream stream) {
		this.stream = stream;
		this.node = node;
	}

	public void log(final String s) {
		stream.println("[" + node.getAddress() + "]:" + s);
	}
}
