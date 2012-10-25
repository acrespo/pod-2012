package ar.edu.itba.pod.legajo51190.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.View;

import ar.edu.itba.pod.api.Signal;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

public class Node implements JGroupNode {
	private ConcurrentSkipListSet<Address> aliveNodes;
	private ConcurrentSkipListSet<String> aliveNodeNames;
	private View lastView;
	private Address nodeAddress;
	private final AtomicBoolean isDegraded = new AtomicBoolean(false);
	private final BlockingQueue<Signal> signals;
	private final Channel channel;

	public Node(final BlockingQueue<Signal> signals, final Channel channel) {
		super();
		this.signals = signals;
		this.channel = channel;
	}

	@Override
	public Address getAddress() {
		return nodeAddress;
	}

	@Override
	public ConcurrentSkipListSet<String> getAliveNodeNames() {
		return aliveNodeNames;
	}

	@Override
	public ConcurrentSkipListSet<Address> getAliveNodes() {
		return aliveNodes;
	}

	public Channel getChannel() {
		return channel;
	}

	@Override
	public AtomicBoolean getIsDegraded() {
		return isDegraded;
	}

	@Override
	public View getLastView() {
		return lastView;
	}

	public BlockingQueue<Signal> getSignals() {
		return signals;
	}

	public void setDegraded(final boolean b) {
		isDegraded.set(b);
	}

	public void setNodeAddress(final Address address) {
		nodeAddress = address;
	}

	public void setNodeView(final View view) {
		aliveNodes = new ConcurrentSkipListSet<>(view.getMembers());
		aliveNodeNames = new ConcurrentSkipListSet<String>(
				Collections2.transform(aliveNodes,
						new Function<Address, String>() {
							@Override
							public String apply(@Nullable final Address input) {
								return input.toString();
							}
						}));
		lastView = view;
	}
}
