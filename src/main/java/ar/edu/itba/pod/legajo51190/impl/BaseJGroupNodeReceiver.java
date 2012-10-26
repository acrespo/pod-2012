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

	}

	@Override
	public void getState(final OutputStream output) throws Exception {

	}

	@Override
	public void setState(final InputStream input) throws Exception {

	}

	@Override
	public void viewAccepted(final View new_view) {

	}

	@Override
	public void suspect(final Address suspected_mbr) {

	}

	@Override
	public void block() {

	}

	@Override
	public void unblock() {

	}

	@Override
	public void channelConnected(final Channel channel) {

	}

	@Override
	public void channelDisconnected(final Channel channel) {
		System.out.println("Disconnected!");
	}

	@Override
	public void channelClosed(final Channel channel) {
		System.out.println("Closed!");
	}
}
