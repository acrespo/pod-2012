package ar.edu.itba.pod.legajo51190.impl;

import java.util.concurrent.CountDownLatch;

import org.jgroups.Channel;
import org.jgroups.ChannelListener;

/**
 * Listener for synchronizing a set of nodes when all everyone has joined or
 * someone has left.
 * 
 * Useful for doing things like: Take a node out, and when he leaves do
 * something. Or add 10 nodes and then they join, do something.
 * 
 * @author cris
 */
public class SyncListener implements ChannelListener, NodeListener {

	private CountDownLatch disconnectionLatch = null;
	private CountDownLatch connectionLatch = null;
	private CountDownLatch newNodeLatch = null;
	private CountDownLatch goneMemberLatch = null;

	public void setNewNodeLatch(final CountDownLatch newNodeLatch) {
		this.newNodeLatch = newNodeLatch;
	}

	public void setDisconnectionLatch(final CountDownLatch disconnectionLatch) {
		this.disconnectionLatch = disconnectionLatch;
	}

	public void setConnectionLatch(final CountDownLatch connectionLatch) {
		this.connectionLatch = connectionLatch;
	}

	public void setGoneMemberLatch(final CountDownLatch goneMemberLatch) {
		this.goneMemberLatch = goneMemberLatch;
	}

	@Override
	public void channelDisconnected(final Channel channel) {
		if (disconnectionLatch != null) {
			disconnectionLatch.countDown();
		}
	}

	@Override
	public void channelConnected(final Channel channel) {
		if (connectionLatch != null) {
			connectionLatch.countDown();
		}
	}

	@Override
	public void channelClosed(final Channel channel) {

	}

	@Override
	public void onNodeSyncDone() {
		if (newNodeLatch != null) {
			newNodeLatch.countDown();
		}
	}

	@Override
	public void onNodeGoneSyncDone() {
		if (goneMemberLatch != null) {
			goneMemberLatch.countDown();
		}
	}

	public CountDownLatch getNewNodeAwaitLatch() {
		return newNodeLatch;
	}

}
