package ar.edu.itba.pod.legajo51190.impl;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.View;

import ar.edu.itba.pod.api.NodeStats;
import ar.edu.itba.pod.api.Signal;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

public class Node implements JGroupNode {
	private final Multimap<Address, Signal> backupSignals;
	private ConcurrentSkipListSet<Address> aliveNodes;
	private ConcurrentSkipListSet<String> aliveNodeNames;
	private View lastView;
	private Address nodeAddress;
	private final AtomicBoolean isDegraded = new AtomicBoolean(false);
	private final Set<Signal> signals;
	private final Set<Signal> toDistributeSignals;

	private final Channel channel;
	private final NodeListener listener;

	public Node(final Set<Signal> signals, final Channel channel,
			final Set<Signal> toDistributeSignals, final NodeListener listener) {
		super();
		this.signals = signals;
		this.channel = channel;
		this.toDistributeSignals = toDistributeSignals;
		this.listener = listener;
		Multimap<Address, Signal> sig = HashMultimap.create();
		backupSignals = Multimaps.synchronizedMultimap(sig);
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

	@Override
	public Set<Signal> getLocalSignals() {
		return signals;
	}

	@Override
	public Set<Signal> getToDistributeSignals() {
		return toDistributeSignals;
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

	@Override
	public Multimap<Address, Signal> getBackupSignals() {
		return backupSignals;
	}

	@Override
	public NodeStats getStats() {
		// TODO: Improve nodestats implementation
		return new NodeStats(getAddress().toString(), 0, signals.size(),
				backupSignals.size(), false);
	}

	public NodeListener getListener() {
		return listener;
	}
}
