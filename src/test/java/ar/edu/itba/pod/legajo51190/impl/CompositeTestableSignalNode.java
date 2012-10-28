package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;

import ar.edu.itba.pod.api.NodeStats;
import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.SPNode;
import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.api.SignalProcessor;

/**
 * SignalNode implementation for testing a SPNode and SignalProcessor Requires
 * the use of a {@link CountdownSyncListener} passed as parameter into the
 * {@link SignalProcessor} in order to listen to the connection and
 * disconnection of a channel. Otherwise, join and exit methods MUST be
 * blocking.
 * 
 * @author cris
 * 
 */
public class CompositeTestableSignalNode implements SignalNode {

	private final SPNode node;
	private final SignalProcessor processor;
	private final CountdownSyncListener injectedListener;

	public CompositeTestableSignalNode(final SPNode node,
			final SignalProcessor processor,
			final CountdownSyncListener injectedListener) {
		this.node = node;
		this.processor = processor;
		this.injectedListener = injectedListener;
	}

	@Override
	public void add(final Signal signal) throws RemoteException {
		processor.add(signal);
	}

	@Override
	public Result findSimilarTo(final Signal signal) throws RemoteException {
		return processor.findSimilarTo(signal);
	}

	@Override
	public void join(final String clusterName) throws RemoteException {
		node.join(clusterName);
	}

	@Override
	public void exit() throws RemoteException {
		node.exit();
	}

	@Override
	public NodeStats getStats() throws RemoteException {
		return node.getStats();
	}

	@Override
	public JGroupNode getJGroupNode() {
		return ((MultiThreadedSignalProcessor) processor).getJGroupNode();
	}

	@Override
	public CountdownSyncListener getInjectedListener() {
		return injectedListener;
	}
}
