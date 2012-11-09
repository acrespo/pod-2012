package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;

import ar.edu.itba.pod.api.NodeStats;
import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.SPNode;
import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.api.SignalProcessor;

/**
 * SignalNode implementation for testing a SPNode and SignalProcessor Requires
 * the use of a {@link SyncListener} passed as parameter into the
 * {@link SignalProcessor} in order to listen to the connection and
 * disconnection of a channel. Otherwise, join and exit methods MUST be
 * blocking.
 * 
 * @author cris
 * 
 */
public class CompositeTestableSignalNode implements SignalNode {

	private final JGroupSignalProcessor processor;
	private final SPNode node;
	private final SignalProcessor signalProcessor;
	private final SyncListener injectedListener;

	public CompositeTestableSignalNode(final JGroupSignalProcessor processor,
			final SyncListener injectedListener) {
		this.processor = processor;
		signalProcessor = (SignalProcessor) processor;
		node = (SPNode) processor;
		this.injectedListener = injectedListener;
	}

	@Override
	public void add(final Signal signal) throws RemoteException {
		signalProcessor.add(signal);
	}

	@Override
	public Result findSimilarTo(final Signal signal) throws RemoteException {
		return signalProcessor.findSimilarTo(signal);
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
		return processor.getJGroupNode();
	}

	@Override
	public SyncListener getInjectedListener() {
		return injectedListener;
	}
}
