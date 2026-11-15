"""
Where solution code to HW5 should be written.  No other files should
be modified.
"""

import socket
import io
import time
import typing
import struct
import homework5
import homework5.logging


def send(sock: socket.socket, data: bytes):
    """
    Implementation of the sending logic for sending data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.
        data -- A bytes object, containing the data to send over the network.
    """

    # Naive implementation where we chunk the data to be sent into
    # packets as large as the network will allow, and then send them
    # over the network, pausing half a second between sends to let the
    # network "rest" :)
    logger = homework5.logging.get_logger("hw5-sender")

    HEADER = struct.Struct("!BI")
    DATA = 1
    ACK = 2
    FIN = 3
    FINACK = 4

    chunk_size = homework5.MAX_PACKET - HEADER.size
    chunks = [data[i:i + chunk_size]
              for i in range(0, len(data), chunk_size)]
   
    window_size = 2
    next_seq = 0
    unacked = {}

    estimated_rtt = 0.1
    dev_rtt = 0.05
    timeout = estimated_rtt + 4 * dev_rtt

    alpha = 0.125
    beta = 0.25

    while next_seq < len(chunks) or unacked:

        # Send new packets while there is room in the window.
        while next_seq < len(chunks) and len(unacked) < window_size:
            packet = HEADER.pack(DATA, next_seq) + chunks[next_seq]

            sock.send(packet)
            unacked[next_seq] = {
                "packet": packet,
                "sent": time.monotonic(),
                "timeout": timeout,
                "retransmitted": False
            }

            logger.info("Sent DATA packet %d", next_seq)
            next_seq += 1

        # Wait for an acknowledgment.
        if unacked:
            now = time.monotonic()
            wait_time = min(
                max(0.001, p["sent"] + p["timeout"] - now)
                for p in unacked.values()
            )

            sock.settimeout(wait_time)

            try:
                response = sock.recv(homework5.MAX_PACKET)

                if len(response) >= HEADER.size:
                    packet_type, seq = HEADER.unpack(
                        response[:HEADER.size]
                    )

                    if packet_type == ACK and seq in unacked:
                        packet_info = unacked.pop(seq)

                        # Update RTT only if the packet wasn't retransmitted.
                        if not packet_info["retransmitted"]:
                            sample_rtt = (
                                time.monotonic() - packet_info["sent"]
                            )

                            dev_rtt = (
                                (1 - beta) * dev_rtt
                                + beta * abs(estimated_rtt - sample_rtt)
                            )

                            estimated_rtt = (
                                (1 - alpha) * estimated_rtt
                                + alpha * sample_rtt
                            )

                            timeout = estimated_rtt + 4 * dev_rtt
                            timeout = max(0.05, min(timeout, 3.0))

                        logger.info("Received ACK %d", seq)

            except socket.timeout:
                pass

        # Retransmit packets whose timers have expired.
        now = time.monotonic()

        for seq, packet_info in list(unacked.items()):
            if now >= packet_info["sent"] + packet_info["timeout"]:
                sock.send(packet_info["packet"])

                packet_info["sent"] = now
                packet_info["timeout"] = min(
                    packet_info["timeout"] * 2, 3.0
                )
                packet_info["retransmitted"] = True

                logger.info("Retransmitting packet %d", seq)

    # Send FIN after every DATA packet has been acknowledged.
    fin_packet = HEADER.pack(FIN, len(chunks))

    while True:
        sock.send(fin_packet)
        sock.settimeout(timeout)

        try:
            response = sock.recv(homework5.MAX_PACKET)

            if len(response) >= HEADER.size:
                packet_type, seq = HEADER.unpack(
                    response[:HEADER.size]
                )

                if packet_type == FINACK and seq == len(chunks):
                    logger.info("Received FINACK")
                    sock.send(HEADER.pack(ACK, seq))
                    break

        except socket.timeout:
            logger.info("Retransmitting FIN")


def recv(sock: socket.socket, dest: io.BufferedIOBase) -> int:
    """
    Implementation of the receiving logic for receiving data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.

    Return:
        The number of bytes written to the destination.
    """
    logger = homework5.logging.get_logger("hw5-receiver")
    # Naive solution, where we continually read data off the socket
    # until we don't receive any more data, and then return.

    HEADER = struct.Struct("!BI")
    DATA = 1
    ACK = 2
    FIN = 3
    FINACK = 4
    
    expected_seq = 0
    received = {}
    finished = False
    closing = False

    num_bytes = 0
    while not finished:
        try:
            data = sock.recv(homework5.MAX_PACKET)
        except socket.timeout:
            if closing:
                break
            continue

        if len(data) < HEADER.size:
            continue

        packet_type, seq = HEADER.unpack(data[:HEADER.size])
        payload = data[HEADER.size:]

        if packet_type == DATA:
            if seq < expected_seq:
                sock.send(HEADER.pack(ACK, seq))

            elif expected_seq <= seq < expected_seq + 2:
                received[seq] = payload
                sock.send(HEADER.pack(ACK, seq))

            while expected_seq in received:
                chunk = received.pop(expected_seq)
                logger.info("Received %d bytes", len(chunk))
                dest.write(chunk)
                num_bytes += len(chunk)
                dest.flush()
                expected_seq += 1

        elif packet_type == FIN:
             if seq == expected_seq:
                sock.send(HEADER.pack(FINACK, seq))
                closing = True 
                sock.settimeout(6.5)
                
        elif packet_type == ACK and closing:
            if seq == expected_seq:
                finished = True


    return num_bytes
