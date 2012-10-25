package ar.edu.itba.pod.legajo51190.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;

import ar.edu.itba.pod.api.Signal;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class NodeUpdateService {

	private final Multimap<Address, Signal> signalsKeptAsBackup = HashMultimap
			.create();
	private final BlockingQueue<Address> waitingAddresses = new LinkedBlockingQueue<>();
	private final ThreadPoolExecutor changeRequestService;
	private final Node node;
	private CountDownLatch latch;
	private final Timer dataUpdateTimer;
	private final AtomicBoolean timerEnabled = new AtomicBoolean(true);

	public NodeUpdateService(final Node node) {
		this.node = node;
		changeRequestService = new ThreadPoolExecutor(1, 1, 1,
				TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>()) {
			@Override
			protected void afterExecute(final Runnable r, final Throwable t) {
				node.setDegraded(getQueue().isEmpty());
				timerEnabled.set(getQueue().isEmpty());
			}
		};

		dataUpdateTimer = new Timer();
		// dataUpdateTimer.scheduleAtFixedRate(new TimerTask() {
		// @Override
		// public void run() {
		//
		// if (timerEnabled.get() && node.getChannel().isConnected()
		// && node.getAliveNodes().size() > 1) {
		// int k = 5000;
		// int localSignalCount = node.getSignals().size();
		// final int amountToDistributePerNode = (int) Math
		// .ceil(new Double(localSignalCount)
		// / node.getAliveNodes().size());
		// int amountOfNodesToTakeFromQueue = amountToDistributePerNode
		// * (node.getAliveNodes().size() - 1);
		// final List<Signal> signalsCopy = new ArrayList<>();
		// synchronized (node.getSignals()) {
		// Iterator<Signal> it = node.getSignals().iterator();
		// int sigsSize = node.getSignals().size();
		// for (int i = 0; i < k
		// && i < sigsSize - amountOfNodesToTakeFromQueue; i++) {
		// signalsCopy.add(it.next());
		// }
		// }
		//
		// System.out.println("-----");
		// System.out.println(amountOfNodesToTakeFromQueue);
		// System.out.println(amountToDistributePerNode);
		// System.out.println(signalsCopy.size());
		// synchronized (dataUpdateTimer) {
		// if (signalsCopy.size() > 0 && timerEnabled.get()) {
		// changeRequestService.submit(new Runnable() {
		// @Override
		// public void run() {
		// try {
		// System.out.println("Sending messages");
		// Set<Address> addresses = new HashSet<Address>(
		// node.getAliveNodes());
		// addresses.remove(node.getAddress());
		// sendSignalsToMembers(addresses,
		// amountToDistributePerNode,
		// signalsCopy);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// }
		// });
		// }
		// }
		// }
		// }
		// }, 0, 1000);
	}

	public void updateFromView(final View new_view) {
		changeRequestService.submit(new Runnable() {
			@Override
			public void run() {
				Set<Address> newMembers = detectNewMembers(new_view);
				Set<Address> goneMembers = detectGoneMembers(new_view);

				synchronized (dataUpdateTimer) {
					timerEnabled.set(false);
					syncGoneMembers(goneMembers);
					try {
						syncNewMembers(newMembers, new_view.getMembers());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				node.setNodeView(new_view);
			}

		});
	}

	private void syncNewMembers(final Set<Address> newMembers,
			final List<Address> allMembers) {

		List<Signal> signalsCopy = null;
		synchronized (node.getSignals()) {
			signalsCopy = new ArrayList<>(node.getSignals());
		}

		int localSignalCount = signalsCopy.size();
		int amountToDistributePerNode = (int) Math.ceil(new Double(
				localSignalCount) / allMembers.size());
		int amountOfNodes = newMembers.size();
		int amountOfNodesToTakeFromQueue = amountToDistributePerNode
				* amountOfNodes;

		List<Signal> signalsTaken = new ArrayList<Signal>();
		Iterator<Signal> it = signalsCopy.iterator();

		for (int i = 0; i < amountOfNodesToTakeFromQueue; i++) {
			signalsTaken.add(it.next());
		}

		sendSignalsToMembers(newMembers, amountToDistributePerNode,
				signalsTaken);

	}

	private void sendSignalsToMembers(final Set<Address> newMembers,
			final int amountToDistributePerNode, final List<Signal> signalsTaken) {
		Multimap<Address, Signal> signalsToSend = HashMultimap.create();

		int c = 0;
		for (Address address : newMembers) {
			for (int i = 0; i < amountToDistributePerNode; i++) {
				signalsToSend.put(address, signalsTaken.get(c++));
			}
		}

		latch = new CountDownLatch(signalsToSend.keySet().size());

		for (Address addr : signalsToSend.keySet()) {
			try {
				Message msg = new Message(addr, new NodeMessage(
						new ArrayList<Signal>(signalsToSend.get(addr)),
						NodeMessage.MESSAGE_NEW_NODE_SYNC));
				signalsKeptAsBackup.putAll(addr, signalsToSend.get(addr));
				waitingAddresses.add(addr);
				node.getChannel().send(msg);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			latch.await(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// waitingAddresses will contain the hanged addresses.
			e.printStackTrace();
		}

		synchronized (node.getSignals()) {
			for (Signal sign : signalsTaken) {
				node.getSignals().remove(sign);
			}
			// syncGoneMembers(new HashSet<Address>(waitingAddresses));
		}

		System.out.println("Now I have " + node.getSignals().size()
				+ " signals");
	}

	private void syncGoneMembers(final Set<Address> goneMembers) {
		if (goneMembers.size() > 1) {
			System.out
					.println("More than one member is gone, we might have lost messages :(");
		}
		synchronized (node.getSignals()) {
			for (Address address : goneMembers) {
				node.getSignals().addAll(signalsKeptAsBackup.get(address));
			}
		}
	}

	private Set<Address> detectGoneMembers(final View new_view) {
		Set<Address> goneMembers = new HashSet<Address>();
		for (Address address : node.getLastView().getMembers()) {
			if (!new_view.containsMember(address)) {
				// System.out.println("Hold member! bye! " + address);
				node.setDegraded(true);
				goneMembers.add(address);
			}
		}
		return goneMembers;
	}

	private Set<Address> detectNewMembers(final View new_view) {
		Set<Address> newMembers = new HashSet<Address>();
		for (Address address : new_view.getMembers()) {
			if (!node.getLastView().containsMember(address)) {
				// System.out.println("New member! hey! " + address);
				node.setDegraded(true);
				newMembers.add(address);
			}
		}
		return newMembers;
	}

	public void notifyNodeAnswer(final NodeMessage message) {
		waitingAddresses.remove(message.getContent());
		latch.countDown();
	}
}
