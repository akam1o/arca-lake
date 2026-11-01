# Raw TCP between the two hosts (iperf 3.20, 10 s runs, MTU 1500)

Measured on 2026-11-01 between host A and host B over their LAN interface
(virtio-net, KVM guests; speed not reported by the hypervisor). ICMP RTT
B→A: min/avg/max 0.159/0.188/0.270 ms over 20 probes.

| Direction | Streams | Sender Gbit/s | Receiver Gbit/s | Retransmits | iperf CPU sender % | iperf CPU receiver % |
| --- | --- | --- | --- | --- | --- | --- |
| B → A | 1 | 22.4 | 22.4 | 0 | 96.8 | 55.4 |
| B → A | 4 | 20.1 | 20.1 | 0 | 105.2 | 107.8 |
| A → B | 1 | 20.3 | 20.3 | 0 | n/a | n/a |
| A → B | 4 | 21.3 | 21.3 | 123 | n/a | n/a |

A single TCP stream saturates one vCPU on the sender; the link is
CPU-bound, not NIC-bound, at about 20 Gbit/s. Commands: `iperf3 -s -1` on
the server; `iperf3 -c <server> -t 10 [-P 4] [-R] -J` on the client.
