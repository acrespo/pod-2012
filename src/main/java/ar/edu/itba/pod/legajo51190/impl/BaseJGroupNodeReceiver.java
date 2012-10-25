package ar.edu.itba.pod.legajo51190.impl;

import java.io.InputStream;
import java.io.OutputStream;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.View;

/**
 * Stub abstract class representing a node
 * 
 * @author cris
 * 
 */
public class BaseJGroupNodeReceiver implements JGroupNodeReceiver {

	@Override
	public void receive(final Message msg) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getState(final OutputStream output) throws Exception {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void setState(final InputStream input) throws Exception {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void viewAccepted(final View new_view) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void suspect(final Address suspected_mbr) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void block() {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void unblock() {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void channelConnected(final Channel channel) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void channelDisconnected(final Channel channel) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void channelClosed(final Channel channel) {
		throw new RuntimeException("Not implemented");
	}
}
