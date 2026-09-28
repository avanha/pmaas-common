// Package discovery implements peer discovery over UDP multicast: a node can announce the
// entities it knows about to its local network segment, and other nodes can listen for those
// announcements and query for them on startup. It's a generic, protocol-agnostic pub/sub
// primitive (Transport) plus a thin JSON envelope layer on top (see protocol.go, service.go) -
// no plugin- or entity-specific knowledge lives here.
package discovery

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"

	"golang.org/x/net/ipv4"
)

// InstanceID uniquely identifies one running process for its lifetime. Multicast delivers a
// sent packet to every socket joined to the group on the local machine, including the
// sender's own, so every frame carries the sender's InstanceID and Transport drops any packet
// whose InstanceID matches its own before it ever reaches a caller - "don't listen to
// ourselves" is handled once, here, rather than by every consumer of Transport.
type InstanceID string

// NewInstanceID generates a random InstanceID, unique enough that two processes started at
// the same moment on the same machine (e.g. two plugin instances during local development)
// won't collide.
func NewInstanceID() InstanceID {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand.Read only fails if the OS entropy source is unavailable, which would
		// itself be a fatal environment problem well beyond this package's scope to recover
		// from - panicking surfaces that immediately rather than silently handing out
		// colliding/predictable instance IDs.
		panic(fmt.Sprintf("discovery: unable to generate instance id: %v", err))
	}
	return InstanceID(hex.EncodeToString(buf))
}

// Packet is one received, already self-filtered message.
type Packet struct {
	Sender InstanceID
	From   *net.UDPAddr
	Data   []byte
}

// frame is the wire format Transport actually sends: an InstanceID wrapper around an
// opaque payload. json encodes/decodes a []byte field as base64 automatically, so this stays
// simple without hand-rolled framing.
type frame struct {
	Sender InstanceID `json:"sender"`
	Data   []byte     `json:"data"`
}

// Config controls how a Transport joins and uses a multicast group.
type Config struct {
	// GroupAddress is the multicast group and port, e.g. "239.192.42.1:9999". Must be in the
	// administratively-scoped range 239.0.0.0/8-239.255.255.255 for LAN-local traffic that
	// should never be routed beyond the local network segment.
	GroupAddress string

	// InterfaceName pins the Transport to one network interface, by name (e.g. "en0"). Left
	// empty, the OS chooses a default, which on a multi-homed host (a Mac with Wi-Fi plus an
	// active VPN/utun interface, for example) is frequently the wrong one - both for which
	// interface joins the group and, on send, for which interface a packet actually goes out.
	// Explicitly setting this is strongly recommended whenever more than one interface is
	// likely to be up, and required for reliable delivery in practice, not just an edge case.
	InterfaceName string

	// EnableSend and EnableReceive are independent, per the requirement that a node can
	// announce without listening for others, or listen without ever announcing itself. Both
	// default to false; NewTransport rejects a Config with neither set, since that Transport
	// would do nothing.
	EnableSend    bool
	EnableReceive bool

	// TTL is the IP TTL (hop count) on sent packets, ignored if EnableSend is false. Left at
	// its zero value, the OS default of 1 applies, meaning a packet dies at the first router
	// it reaches and can never leave the local network segment - by far the safest default for
	// administratively-scoped multicast, and the reason this can default to zero rather than
	// needing every caller to set it explicitly.
	//
	// Set above 1 only if this traffic is meant to cross a router onto another subnet/VLAN on
	// the same site, and that router has been explicitly configured for it - a plain L2 switch
	// feature like IGMP snooping/querier does NOT enable this (it only affects delivery within
	// one subnet), and neither does IGMP snooping working correctly for other on-subnet
	// multicast traffic (e.g. Sonos) on the same network. Crossing a router hop needs actual
	// multicast routing (PIM) or an IGMP-proxy feature bridging the two segments (some
	// consumer/prosumer routers, e.g. MikroTik RouterOS 7, support this) - and even then, TTL
	// must be high enough to survive every hop the packet needs to cross (2 for one router
	// hop, and so on), since every router decrements it by 1 and drops the packet outright if
	// that reaches 0, regardless of how well multicast routing is otherwise configured.
	TTL int
}

// Transport is a raw multicast pub/sub primitive: send/receive byte payloads to/from a
// multicast group, with sender self-filtering built in. It knows nothing about entities or
// message semantics beyond a sender InstanceID - see protocol.go/service.go for the envelope
// format and higher-level API built on top of it.
//
// It's built on golang.org/x/net/ipv4 rather than net.UDPConn's own Write/WriteTo, because
// net.ListenMulticastUDP's interface argument only controls IGMP group membership, not which
// interface a subsequent send actually goes out on - on a multi-homed host that send silently
// picks whatever interface the OS's default multicast route happens to be (or fails outright
// with "can't assign requested address" if there isn't one), regardless of InterfaceName.
// ipv4.PacketConn.SetMulticastInterface is the documented way to pin outgoing multicast
// traffic to a specific interface, which is why this depends on x/net rather than staying
// stdlib-only.
type Transport struct {
	instanceID InstanceID
	config     Config
	groupAddr  *net.UDPAddr

	// recvPC and sendPC may be the same *ipv4.PacketConn (when both directions are enabled,
	// one socket serves both) or distinct (send-only doesn't join the group at all, so it
	// never receives group traffic it has no interest in), or either may be nil if that
	// direction isn't enabled.
	recvPC *ipv4.PacketConn
	sendPC *ipv4.PacketConn

	packetCh  chan Packet
	doneCh    chan struct{}
	closeOnce sync.Once
}

// NewTransport creates and, if Config.EnableReceive is set, starts a Transport. instanceID
// should be generated once per process (see NewInstanceID) and reused for every Transport
// that process creates, so self-filtering works consistently across all of them.
func NewTransport(instanceID InstanceID, config Config) (*Transport, error) {
	if !config.EnableSend && !config.EnableReceive {
		return nil, errors.New("discovery: transport must enable send, receive, or both")
	}

	groupAddr, err := net.ResolveUDPAddr("udp4", config.GroupAddress)
	if err != nil {
		return nil, fmt.Errorf("discovery: invalid group address %q: %w", config.GroupAddress, err)
	}

	var iface *net.Interface
	if config.InterfaceName != "" {
		iface, err = net.InterfaceByName(config.InterfaceName)
		if err != nil {
			return nil, fmt.Errorf("discovery: unknown interface %q: %w", config.InterfaceName, err)
		}
	}

	t := &Transport{
		instanceID: instanceID,
		config:     config,
		groupAddr:  groupAddr,
		packetCh:   make(chan Packet, 32),
		doneCh:     make(chan struct{}),
	}

	if config.EnableReceive {
		udpConn, err := net.ListenMulticastUDP("udp4", iface, groupAddr)
		if err != nil {
			return nil, fmt.Errorf("discovery: unable to join multicast group %s: %w", config.GroupAddress, err)
		}

		pc := ipv4.NewPacketConn(udpConn)
		if iface != nil {
			if err := pc.SetMulticastInterface(iface); err != nil {
				_ = udpConn.Close()
				return nil, fmt.Errorf("discovery: unable to set multicast interface %q: %w", config.InterfaceName, err)
			}
		}

		t.recvPC = pc
		if config.EnableSend {
			t.sendPC = pc
		}
		go t.receiveLoop()
	} else {
		// No receive loop will ever run to close this, so it starts already-closed - Close()
		// waits on it unconditionally regardless of which directions are enabled.
		close(t.doneCh)
	}

	if config.EnableSend && t.sendPC == nil {
		// Send-only deliberately doesn't join the group (no net.ListenMulticastUDP call) -
		// it only ever writes, so there's no reason to also subscribe this socket to incoming
		// group traffic at the OS/IGMP level.
		udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{})
		if err != nil {
			return nil, fmt.Errorf("discovery: unable to open send socket: %w", err)
		}

		pc := ipv4.NewPacketConn(udpConn)
		if iface != nil {
			if err := pc.SetMulticastInterface(iface); err != nil {
				_ = udpConn.Close()
				return nil, fmt.Errorf("discovery: unable to set multicast interface %q: %w", config.InterfaceName, err)
			}
		}
		t.sendPC = pc
	}

	if config.EnableSend && config.TTL > 0 {
		if err := t.sendPC.SetMulticastTTL(config.TTL); err != nil {
			t.closeSockets()
			return nil, fmt.Errorf("discovery: unable to set multicast TTL %d: %w", config.TTL, err)
		}
	}

	return t, nil
}

// closeSockets is NewTransport's own cleanup path for an error after one or both sockets have
// already been opened - unlike Close, it doesn't wait on doneCh (the receive loop, if any,
// hasn't been started yet at any point this is called) and isn't guarded by closeOnce, since
// NewTransport never returns a *Transport for a caller to Close in this case.
func (t *Transport) closeSockets() {
	if t.recvPC != nil {
		_ = t.recvPC.Close()
	}
	if t.sendPC != nil && t.sendPC != t.recvPC {
		_ = t.sendPC.Close()
	}
}

// Publish sends payload to the multicast group. Returns an error if this Transport wasn't
// configured with EnableSend.
func (t *Transport) Publish(payload []byte) error {
	if !t.config.EnableSend {
		return errors.New("discovery: transport is not configured to send")
	}

	data, err := json.Marshal(frame{Sender: t.instanceID, Data: payload})
	if err != nil {
		return fmt.Errorf("discovery: unable to encode frame: %w", err)
	}

	if _, err := t.sendPC.WriteTo(data, nil, t.groupAddr); err != nil {
		return fmt.Errorf("discovery: unable to send to %s: %w", t.config.GroupAddress, err)
	}
	return nil
}

// Packets returns the channel of received, self-filtered packets. Closed once Close has
// fully torn down the Transport. Callers must keep up with it - see receiveLoop.
func (t *Transport) Packets() <-chan Packet {
	return t.packetCh
}

func (t *Transport) receiveLoop() {
	defer close(t.doneCh)
	buf := make([]byte, 65535)

	for {
		n, _, from, err := t.recvPC.ReadFrom(buf)
		if err != nil {
			// Close() closing recvPC is what unblocks this read in the normal shutdown case;
			// any other error is treated the same way, since a UDP socket read error
			// generally isn't recoverable in place.
			return
		}

		var f frame
		if err := json.Unmarshal(buf[:n], &f); err != nil {
			fmt.Printf("discovery: dropping malformed frame from %s: %v\n", from, err)
			continue
		}

		if f.Sender == t.instanceID {
			// Don't listen to ourselves: multicast loops back to every socket joined to the
			// group on this machine, including our own.
			continue
		}

		udpFrom, _ := from.(*net.UDPAddr)

		select {
		case t.packetCh <- Packet{Sender: f.Sender, From: udpFrom, Data: f.Data}:
		default:
			// The consumer isn't keeping up. Drop rather than block: a stalled receive loop
			// would eventually cause the OS socket buffer to fill and start silently dropping
			// datagrams anyway, and blocking here would also risk Close() never being able to
			// unblock a pending ReadFrom. Consumers that need every message should drain
			// Packets() promptly and keep their own handling of each one fast - the same
			// requirement already placed on EventManager receivers in this codebase.
			fmt.Printf("discovery: dropping packet from %s, receiver not keeping up\n", f.Sender)
		}
	}
}

// Close stops the Transport: closes its socket(s), which unblocks any in-progress receive,
// waits for the receive loop to exit, and closes Packets(). Safe to call multiple times; only
// the first call does anything.
func (t *Transport) Close() error {
	var err error
	t.closeOnce.Do(func() {
		if t.recvPC != nil {
			err = t.recvPC.Close()
		}
		if t.sendPC != nil && t.sendPC != t.recvPC {
			if sendErr := t.sendPC.Close(); err == nil {
				err = sendErr
			}
		}
		<-t.doneCh
		close(t.packetCh)
	})
	return err
}
