package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.signal.source.RandomSource;

/**
 * Handles a set of signal nodes and their connections.
 * 
 * @author cris
 */
public class NodeTestController {
	private final LinkedList<SignalNode> nodesToTest = new LinkedList<>();
	private final SyncListener listener = new SyncListener();
	private final RandomSource src = new RandomSource(12345);
	private final SignalNodeTestFactory signalNodeTestFactory;

	public NodeTestController(final SignalNodeTestFactory signalNodeTestFactory) {
		this.signalNodeTestFactory = signalNodeTestFactory;
	}

	/**
	 * Must disconnect all processors and block until it's done.
	 */
	public void disconnectAllNodesFromChannel() {

		if (getNodesToTest().size() == 0) {
			return;
		}

		CountDownLatch disconnectionLatch = new CountDownLatch(getNodesToTest()
				.size());
		boolean isBlocking = false;

		getListener().setDisconnectionLatch(disconnectionLatch);

		for (SignalNode node : getNodesToTest()) {
			try {
				isBlocking = node.getInjectedListener() == null;
				node.exit();
			} catch (RemoteException e) {

			}
		}

		if (!isBlocking) {
			try {
				disconnectionLatch.await(10000, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				throw new RuntimeException("Something didn't sync right");
			}
		}
		System.out.println("==================");
		System.out.println("ALL MEMBERS GONE!");
	}

	/**
	 * Must be called at the beggining of a test. Must start the processors in
	 * background and connect all of them to the same channel, blocking until
	 * it's done.
	 * 
	 * @param size
	 *            Amount of nodes to start with
	 */
	public void instanciateNodes(final int size) {
		addNewNodes(size);
	}

	/**
	 * Joins a set of nodes to the testChannel, awaiting if it's blocking a
	 * blocking implementation or synchronizing if it's not
	 * 
	 * @param nodes
	 *            Nodes to add to the channel
	 * @param controlLatch
	 *            Latch for controlling the synchronization
	 */
	public void joinNodesToChannel(final Set<SignalNode> nodes,
			final CountDownLatch controlLatch) {

		boolean isBlocking = false;

		getListener().setConnectionLatch(controlLatch);

		int sum = 0;

		for (int i = nodesToTest.size(); i < nodes.size() + nodesToTest.size(); i++) {
			sum += i;
		}

		if (nodesToTest.size() == 0) {
			sum = 0;
		}

		System.out.println("Awaiting for " + sum);
		CountDownLatch newNodeAwaitLatch = new CountDownLatch(sum);

		getListener().setNewNodeLatch(newNodeAwaitLatch);

		for (SignalNode node : nodes) {
			try {
				isBlocking = node.getInjectedListener() == null
						&& node.getInjectedListener() == getListener();
				node.join("testChannel");
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}

		if (!isBlocking) {
			try {
				if (!controlLatch.await(10, TimeUnit.SECONDS)) {
					throw new InterruptedException();
				}

			} catch (InterruptedException e) {
				throw new RuntimeException("Something didn't sync right");
			}
		}

		try {
			if (!newNodeAwaitLatch.await(30, TimeUnit.SECONDS)) {
				System.out.println("I GOT " + newNodeAwaitLatch.getCount());
				throw new InterruptedException();
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("Something didn't sync right");
		}

	}

	/**
	 * Adds a set of signals to a node, doesn't yet mind for synchronization
	 * 
	 * @param node
	 *            Node to send signals to.
	 * @param amountOfSignals
	 *            Amount of random signals to add
	 */
	public void addSignalsToNode(final SignalNode node,
			final int amountOfSignals) {
		System.out.println("===== Adding " + amountOfSignals + " signals to "
				+ node.getJGroupNode().getAddress());

		CountDownLatch newNodeAwaitLatch = new CountDownLatch(1);

		getListener().setNewNodeLatch(newNodeAwaitLatch);

		for (int i = 0; i < amountOfSignals; i++) {
			try {
				node.add(src.next());
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}

		try {
			if (!newNodeAwaitLatch.await(30, TimeUnit.SECONDS)) {
				throw new InterruptedException();
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("Something didn't sync right");
		}
	}

	public void prepareNodeForSendingSignals(final SignalNode last) {
		CountDownLatch newNodeAwaitLatch = new CountDownLatch(1);
		getListener().setNewNodeLatch(newNodeAwaitLatch);
	}

	public void addSignalToNode(final SignalNode last, final Signal s) {
		try {
			last.add(s);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	public void awaitSignalsSentToNode(final SignalNode last) {
		try {
			if (!getListener().getNewNodeAwaitLatch().await(30,
					TimeUnit.SECONDS)) {
				throw new InterruptedException();
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("Something didn't sync right");
		}
	}

	/**
	 * Add nodes to the channel. Guarantees that all nodes are joined after it
	 * leaves.
	 * 
	 * @param amount
	 *            Amount of nodes to add to the channel.
	 */
	public void addNewNodes(final int amount) {
		Set<SignalNode> nodes = new HashSet<>();
		for (int i = 0; i < amount; i++) {
			nodes.add(signalNodeTestFactory.getNewSignalNode(getListener()));
		}

		CountDownLatch connectionLatch = new CountDownLatch(amount);
		getListener().setConnectionLatch(connectionLatch);

		joinNodesToChannel(nodes, connectionLatch);

		getNodesToTest().addAll(nodes);
	}

	public void removeNode(final SignalNode n) {

		System.out.println("===== Removing node "
				+ n.getJGroupNode().getAddress());

		getNodesToTest().remove(n);

		CountDownLatch newNodeAwaitLatch = new CountDownLatch(
				nodesToTest.size());
		listener.setGoneMemberLatch(newNodeAwaitLatch);

		try {
			n.exit();
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}

		try {
			if (!newNodeAwaitLatch.await(30, TimeUnit.SECONDS)) {
				throw new InterruptedException();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			// throw new RuntimeException("Something didn't sync right");
		}
	}

	public SyncListener getListener() {
		return listener;
	}

	public LinkedList<SignalNode> getNodesToTest() {
		return nodesToTest;
	}
}
