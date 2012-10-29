package ar.edu.itba.pod.legajo51190.impl;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.jgroups.Address;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the synchronization between a set of Nodes instanciated inside a local
 * jGroup
 * 
 * @author cris
 */
public abstract class AbstractDistributedNodeTest {

	private NodeTestController controller;

	@Before
	public void setup() throws Exception {
		controller = new NodeTestController(new SignalNodeTestFactory() {
			@Override
			public SignalNode getNewSignalNode(final SyncListener listener) {
				return createNewSignalNode(listener);
			}
		});
	}

	@After
	public void clear() throws Exception {
		controller.disconnectAllNodesFromChannel();
	}

	public abstract SignalNode createNewSignalNode(SyncListener listener);

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

		for (SignalNode node : controller.getNodesToTest()) {
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

		if (controller.getNodesToTest().size() > 1) {
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
		controller.instanciateNodes(1);

		// We add nodes to the first member and await it to
		SignalNode first = controller.getNodesToTest().getFirst();

		controller.addSignalsToNode(first, 2400);

		controller.addNewNodes(1);

		assertNodeIsNotEmpty(controller.getNodesToTest().getFirst());
		assertNodeIsNotEmpty(controller.getNodesToTest().getLast());
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
		controller.instanciateNodes(1);

		// We add nodes to the first member and await it to
		SignalNode first = controller.getNodesToTest().getFirst();

		controller.addSignalsToNode(first, 1000);

		controller.addNewNodes(2);

		Thread.sleep(10000);

		assertNodeStoreIsNotEmpty(controller.getNodesToTest().getFirst());
		assertNodeStoreIsNotEmpty(controller.getNodesToTest().get(1));
		assertNodeStoreIsNotEmpty(controller.getNodesToTest().getLast());
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

		controller.addNewNodes(1);

		assertNodeIsNotEmpty(controller.getNodesToTest().getFirst());
		assertNodeIsNotEmpty(controller.getNodesToTest().get(1));
		assertNodeIsNotEmpty(controller.getNodesToTest().getLast());
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

		controller.addNewNodes(5);

		Thread.sleep(10000);

		// assertNodeIsNotEmpty(controller.getNodesToTest().getFirst());
		// assertNodeIsNotEmpty(controller.getNodesToTest().get(2));
		// assertNodeIsNotEmpty(controller.getNodesToTest().getLast());
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

		controller.addNewNodes(1);

		assertTotalAmountIs(2400);

		controller.addNewNodes(1);

		assertNodeIsNotEmpty(controller.getNodesToTest().getFirst());
		assertNodeIsNotEmpty(controller.getNodesToTest().get(1));
		assertNodeIsNotEmpty(controller.getNodesToTest().get(2));
		assertNodeIsNotEmpty(controller.getNodesToTest().getLast());
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

		controller.addSignalsToNode(controller.getNodesToTest().getLast(),
				21600);

		assertNodeIsNotEmpty(controller.getNodesToTest().getFirst());
		assertNodeIsNotEmpty(controller.getNodesToTest().get(1));
		assertNodeIsNotEmpty(controller.getNodesToTest().get(2));
		assertNodeIsNotEmpty(controller.getNodesToTest().getLast());
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

		controller.addSignalsToNode(controller.getNodesToTest().getLast(),
				21600);

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

		controller.removeNode(controller.getNodesToTest().getFirst());

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

		controller.removeNode(controller.getNodesToTest().getFirst());

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

		controller.removeNode(controller.getNodesToTest().getFirst());

		Thread.sleep(5000);

		assertTotalAmountIs(24000);
	}
}
