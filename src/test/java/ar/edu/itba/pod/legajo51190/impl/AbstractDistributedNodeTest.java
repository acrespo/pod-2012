package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import ar.edu.itba.pod.legajo51190.api.SignalNode;
import ar.edu.itba.pod.signal.source.RandomSource;

/**
 * Tests the synchronization between a set of Nodes instanciated inside a local
 * jGroup
 * 
 * @author cris
 */
public abstract class AbstractDistributedNodeTest {

	private final CountdownSyncListener listener = new CountdownSyncListener();

	private final LinkedList<SignalNode> nodesToTest = new LinkedList<>();

	private RandomSource src;

	@Before
	public void setup() throws Exception {
		src = new RandomSource(12345);
	}

	public abstract SignalNode createNewSignalNode(
			CountdownSyncListener listener);

	/**
	 * Must disconnect all processors and block until it's done.
	 */
	private void disconnectAllNodesFromChannel() {

		if (nodesToTest.size() == 0) {
			return;
		}

		CountDownLatch disconnectionLatch = new CountDownLatch(
				nodesToTest.size());
		boolean isBlocking = false;

		listener.setDisconnectionLatch(disconnectionLatch);

		for (SignalNode node : nodesToTest) {
			try {
				isBlocking = node.getInjectedListener() == null;
				node.exit();
			} catch (RemoteException e) {

			}
		}

		if (!isBlocking) {
			try {
				disconnectionLatch.await(3, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				throw new RuntimeException("Something didn't sync right");
			}
		}
	}

	/**
	 * Must start the processors in background and connect all of them to the
	 * same channel, blocking until it's done.
	 */
	private void instanciateNodes(final int size) {

		disconnectAllNodesFromChannel();

		nodesToTest.clear();

		addNewNodes(size);
	}

	private void joinNodesToChannel(final Set<SignalNode> nodes,
			final CountDownLatch controlLatch) {

		boolean isBlocking = false;

		listener.setConnectionLatch(controlLatch);

		for (SignalNode node : nodes) {
			try {
				isBlocking = node.getInjectedListener() == null;
				node.join("testChannel");
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}

		if (!isBlocking) {
			try {
				controlLatch.await(3, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				throw new RuntimeException("Something didn't sync right");
			}
		}

	}

	private void addSignalsToNode(final SignalNode node,
			final int amountOfSignals) {
		for (int i = 0; i < amountOfSignals; i++) {
			try {
				node.add(src.next());
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
	}

	private void addNewNodes(final int amount) {
		Set<SignalNode> nodes = new HashSet<>();
		for (int i = 0; i < amount; i++) {
			nodes.add(createNewSignalNode(listener));
		}

		CountDownLatch connectionLatch = new CountDownLatch(amount);
		listener.setConnectionLatch(connectionLatch);

		joinNodesToChannel(nodes, connectionLatch);

		nodesToTest.addAll(nodes);
	}

	private void assertNodeIsNotEmpty(final SignalNode node)
			throws RemoteException {

		Assert.assertTrue(node.getStats().backupSignals() > 0);
		Assert.assertTrue(node.getStats().storedSignals() > 0);

	}

	private void assertTotalAmountIs(final int stored) throws RemoteException {

		int sumStored = 0;
		int sumBackuped = 0;

		for (SignalNode node : nodesToTest) {
			sumStored += node.getStats().storedSignals();
			sumBackuped += node.getStats().backupSignals();
		}

		Assert.assertEquals(sumStored, stored);
		Assert.assertEquals(sumBackuped, stored);

	}

	@Test
	public void synchronizeOnNewMember() throws InterruptedException,
			RemoteException {
		instanciateNodes(1);

		SignalNode first = nodesToTest.getFirst();

		addSignalsToNode(first, 1000);

		Thread.sleep(2000);

		addNewNodes(1);

		Thread.sleep(2000); // Time to sync TODO: Make this synchronizable

		assertNodeIsNotEmpty(nodesToTest.getFirst());
		assertNodeIsNotEmpty(nodesToTest.getLast());
		assertTotalAmountIs(1000);
	}
}
