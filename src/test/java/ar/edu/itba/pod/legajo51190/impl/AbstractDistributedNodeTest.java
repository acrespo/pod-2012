package ar.edu.itba.pod.legajo51190.impl;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.jgroups.Address;
import org.junit.Before;
import org.junit.Test;

import ar.edu.itba.pod.signal.source.RandomSource;

/**
 * Tests the synchronization between a set of Nodes instanciated inside a local
 * jGroup
 * 
 * @author cris
 */
public abstract class AbstractDistributedNodeTest {

	private final SyncListener listener = new SyncListener();

	private static final LinkedList<SignalNode> nodesToTest = new LinkedList<>();

	private RandomSource src;

	@Before
	public void setup() throws Exception {
		src = new RandomSource(12345);
	}

	public abstract SignalNode createNewSignalNode(SyncListener listener);

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
	private void instanciateNodes(final int size) {

		disconnectAllNodesFromChannel();

		nodesToTest.clear();

		// try {
		// // Even though it syncs, sometimes when a new channel is built the
		// // views don't start empty, so we wait 5 seconds here.
		// // Thread.sleep(5000);
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }

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
	private void joinNodesToChannel(final Set<SignalNode> nodes,
			final CountDownLatch controlLatch) {

		boolean isBlocking = false;

		listener.setConnectionLatch(controlLatch);

		for (SignalNode node : nodes) {
			try {
				isBlocking = node.getInjectedListener() == null
						&& node.getInjectedListener() == listener;
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
		//
		// System.out.println("Nodes to test: " + nodesToTest.size() +
		// " nodes");
		// System.out.println("Awaiting for: " + nodes.size() *
		// nodesToTest.size()
		// + " nodes");
		CountDownLatch newNodeAwaitLatch = new CountDownLatch(
				nodes.size() == 0 ? 0 : nodesToTest.size());

		listener.setNewNodeLatch(newNodeAwaitLatch);

		try {
			if (!newNodeAwaitLatch.await(15, TimeUnit.SECONDS)) {
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
	private void addSignalsToNode(final SignalNode node,
			final int amountOfSignals) {
		System.out.println("===== Adding " + amountOfSignals + " signals to "
				+ node.getJGroupNode().getAddress());

		for (int i = 0; i < amountOfSignals; i++) {
			try {
				node.add(src.next());
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}

		CountDownLatch newNodeAwaitLatch = new CountDownLatch(1);

		listener.setNewNodeLatch(newNodeAwaitLatch);

		try {
			if (!newNodeAwaitLatch.await(30, TimeUnit.SECONDS)) {
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

	private void removeNode(final SignalNode n) {

		System.out.println("===== Removing node "
				+ n.getJGroupNode().getAddress());
		nodesToTest.remove(n);
		try {
			n.exit();
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// CountDownLatch newNodeAwaitLatch = new CountDownLatch(
		// nodesToTest.size());
		//
		// listener.setNewNodeLatch(newNodeAwaitLatch);
		//
		// try {
		// if (!newNodeAwaitLatch.await(30, TimeUnit.SECONDS)) {
		// throw new InterruptedException();
		// }
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// // throw new RuntimeException("Something didn't sync right");
		// }
	}

	/**
	 * Asserts that the node has backup signals and own signals
	 * 
	 * @param node
	 *            Node to analyze
	 */
	private void assertNodeIsNotEmpty(final SignalNode node)
			throws RemoteException {
		Assert.assertTrue(node.getStats().backupSignals() > 0);
		Assert.assertTrue(node.getStats().storedSignals() > 0);
	}

	/**
	 * Asserts that the node has backup signals and own signals
	 * 
	 * @param node
	 *            Node to analyze
	 */
	private void assertNodeStoreIsNotEmpty(final SignalNode node)
			throws RemoteException {
		Assert.assertTrue(node.getStats().storedSignals() > 0);
	}

	/**
	 * Test that the total amount of stored elements is the given size.
	 * 
	 * @param stored
	 * @throws RemoteException
	 */
	private void assertTotalAmountIs(final int stored) throws RemoteException {

		int sumStored = 0;
		int sumBackuped = 0;

		Map<Address, Integer> localSize = new HashMap<Address, Integer>();
		Map<Address, Integer> backupSize = new HashMap<Address, Integer>();

		for (SignalNode node : nodesToTest) {
			sumStored += node.getStats().storedSignals();
			sumBackuped += node.getStats().backupSignals();

			JGroupNode jNode = node.getJGroupNode();

			localSize.put(jNode.getAddress(), jNode.getLocalSignals().size());

			NodeLogger logger = new NodeLogger(jNode);

			// logger.setEnabled(false);

			logger.log("=== Local data: " + jNode.getLocalSignals().size());

			for (Address addr : jNode.getBackupSignals().keySet()) {
				if (!backupSize.containsKey(addr)) {
					backupSize.put(addr, 0);
				}

				backupSize.put(addr, backupSize.get(addr)
						+ jNode.getBackupSignals().get(addr).size());

				logger.log("=== Backup data for: " + addr + " - "
						+ jNode.getBackupSignals().get(addr).size());
			}
		}

		if (nodesToTest.size() > 1) {
			for (Address addr : localSize.keySet()) {
				Assert.assertEquals(localSize.get(addr), backupSize.get(addr));
			}

			Assert.assertEquals(stored, sumStored);
			Assert.assertEquals(stored, sumBackuped);
		} else {
			Assert.assertEquals(stored, sumStored);
		}

	}

	/**
	 * One node joins a channel with one node.
	 * 
	 * @throws IOException
	 */
	@Test
	public void synchronizeData1Member() throws InterruptedException,
			IOException {

		// Initial group size: 1 member
		instanciateNodes(1);

		// We add nodes to the first member and await it to
		SignalNode first = nodesToTest.getFirst();

		addSignalsToNode(first, 2400);

		addNewNodes(1);

		assertNodeIsNotEmpty(nodesToTest.getFirst());
		assertNodeIsNotEmpty(nodesToTest.getLast());
		assertTotalAmountIs(2400);
	}

	/**
	 * Two nodes join a channel with one node.
	 * 
	 * @throws IOException
	 */
	@Test
	public void synchronizeData2Members() throws InterruptedException,
			IOException {

		// Initial group size: 1 member
		instanciateNodes(1);

		// We add nodes to the first member and await it to
		SignalNode first = nodesToTest.getFirst();

		addSignalsToNode(first, 1000);

		addNewNodes(2);

		Thread.sleep(10000);

		assertNodeStoreIsNotEmpty(nodesToTest.getFirst());
		assertNodeStoreIsNotEmpty(nodesToTest.get(1));
		assertNodeStoreIsNotEmpty(nodesToTest.getLast());
		assertTotalAmountIs(1000);
	}

	/**
	 * 1 node joins a channel with two nodes syncd.
	 * 
	 * @throws IOException
	 */
	@Test
	public void synchronizeData1Member1Member() throws InterruptedException,
			IOException {
		synchronizeData1Member();

		addNewNodes(1);

		assertNodeIsNotEmpty(nodesToTest.getFirst());
		assertNodeIsNotEmpty(nodesToTest.get(1));
		assertNodeIsNotEmpty(nodesToTest.getLast());
		assertTotalAmountIs(2400);
	}

	/**
	 * 1 node joins a channel with two nodes syncd.
	 * 
	 * @throws IOException
	 */
	@Test
	public void synchronizeData1Member5Members() throws InterruptedException,
			IOException {
		synchronizeData1Member();

		addNewNodes(5);

		Thread.sleep(10000);

		// assertNodeIsNotEmpty(nodesToTest.getFirst());
		// assertNodeIsNotEmpty(nodesToTest.get(2));
		// assertNodeIsNotEmpty(nodesToTest.getLast());
		assertTotalAmountIs(2400);
	}

	/**
	 * 2 node join a channel with two nodes syncd.
	 * 
	 * @throws IOException
	 */
	@Test
	public void synchronizeData1Member1Member1Member()
			throws InterruptedException, IOException {
		synchronizeData1Member();

		addNewNodes(1);

		assertTotalAmountIs(2400);

		addNewNodes(1);

		assertNodeIsNotEmpty(nodesToTest.getFirst());
		assertNodeIsNotEmpty(nodesToTest.get(1));
		assertNodeIsNotEmpty(nodesToTest.get(2));
		assertNodeIsNotEmpty(nodesToTest.getLast());
		assertTotalAmountIs(2400);
	}

	/**
	 * 2 node join a channel with two nodes syncd.
	 * 
	 * @throws IOException
	 */
	@Test
	public void synchronizeData3MembersAndAdd21600Signals()
			throws InterruptedException, IOException {
		synchronizeData1Member1Member1Member();

		addSignalsToNode(nodesToTest.getLast(), 21600);

		assertNodeIsNotEmpty(nodesToTest.getFirst());
		assertNodeIsNotEmpty(nodesToTest.get(1));
		assertNodeIsNotEmpty(nodesToTest.get(2));
		assertNodeIsNotEmpty(nodesToTest.getLast());
		assertTotalAmountIs(24000);
	}

	/**
	 * 2 node join a channel with two nodes syncd.
	 * 
	 * @throws IOException
	 */
	@Test
	public void synchronizeData5MembersAndAdd21600Signals()
			throws InterruptedException, IOException {
		synchronizeData1Member5Members();

		addSignalsToNode(nodesToTest.getLast(), 21600);

		assertTotalAmountIs(24000);
	}

	/**
	 * 2 node join a channel with two nodes syncd.
	 * 
	 * @throws IOException
	 */
	@Test
	public void synchronizeData1Member1Delete() throws InterruptedException,
			IOException {
		synchronizeData1Member();

		removeNode(nodesToTest.getFirst());

		assertTotalAmountIs(2400);
	}

	/**
	 * 2 node join a channel with two nodes syncd.
	 * 
	 * @throws IOException
	 */
	@Test
	public void synchronizeData1Member1Member1Delete()
			throws InterruptedException, IOException {
		synchronizeData1Member1Member();

		removeNode(nodesToTest.getFirst());

		assertTotalAmountIs(2400);
	}

	/**
	 * 2 node join a channel with two nodes syncd.
	 * 
	 * @throws IOException
	 */
	@Test
	public void synchronizeLotsOfMembersAndDelete1()
			throws InterruptedException, IOException {
		synchronizeData5MembersAndAdd21600Signals();

		removeNode(nodesToTest.getFirst());

		Thread.sleep(5000);

		assertTotalAmountIs(24000);
	}
}
