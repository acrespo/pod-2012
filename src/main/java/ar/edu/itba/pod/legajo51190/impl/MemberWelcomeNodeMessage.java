package ar.edu.itba.pod.legajo51190.impl;

import java.util.List;
import java.util.Set;

import org.jgroups.Address;

public class MemberWelcomeNodeMessage extends NodeMessage {

	private static final long serialVersionUID = 519694018589442143L;

	private final Set<Address> newMembers;
	private final List<Address> members;

	public MemberWelcomeNodeMessage(final Set<Address> newMembers,
			final List<Address> members) {
		this.newMembers = newMembers;
		this.members = members;
	}

	public Set<Address> getDestinations() {
		return newMembers;
	}

	public List<Address> getAllMembers() {
		return members;
	}

}
