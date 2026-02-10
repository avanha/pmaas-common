package net

import "net"

func FindFirstIpV4Address(addresses []net.IP) net.IP {
	for _, address := range addresses {
		asIpV4 := address.To4()

		if asIpV4 != nil {
			return asIpV4
		}
	}

	return nil
}

func FindFirstGlobalUnicastIpV6Address(addresses []net.IP) net.IP {
	for _, address := range addresses {
		if address.IsGlobalUnicast() && address.To4() == nil {
			return address
		}
	}

	return nil
}
