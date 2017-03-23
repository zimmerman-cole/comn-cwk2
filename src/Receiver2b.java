import com.sun.xml.internal.bind.v2.runtime.reflect.Lister;

import java.net.*;
import java.io.*;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.LinkedList;

public class Receiver2b
{
    // DEBUG:
    private static final int header_length = 3;
    private static final int max_payload_length = 1024;
    private static final int debug_level = 2;
    private static final boolean map_debug = true;

    private static short window_size;
    private static DatagramSocket receiver_socket;
    private static String file_to_write;
    private static byte[] received_data = new byte[0];

    // Contains window positions of received packets
    private static HashMap<Short, byte[]> tempBuffer;
    private static LinkedHashMap<Short, Short> seqToPos;


    public static void main(String[] args) {

        int num_duplicates = 0;

        int portNumber = Integer.parseInt(args[0]);
        file_to_write = args[1];
        window_size = (short) Integer.parseInt(args[2]);
        if (window_size > 16383 || window_size <= 0) {
            System.out.println("Please enter a window size N such that 0 < N <= 16383.");
            System.exit(0);
        }
        try {
            receiver_socket = new DatagramSocket(portNumber);
        } catch (SocketException e) {
            e.printStackTrace();
            System.out.println("SocketException occurred while initializing receiver socket (port # already in use?).");
            quit();
        }

        // A PacketTracker contains all relevant info (sequence number, window position, etc.) for a given packet.
        // This list maps sequence numbers to PacketTrackers, tracking all packets in the current window.
        LinkedHashMap<Short, PacketTracker> window_packets = new LinkedHashMap<Short, PacketTracker>(window_size);
        // Initialize list
        for (int i = 1; i < window_size; i++) {
            window_packets.put((short) i, new PacketTracker((short) i, i));
        }

        // For handling received packets
        byte[] ack_data = new byte[6];
        System.arraycopy("ACK".getBytes(), 0, ack_data, header_length, 3);
        short receivedSeq;
        short new_rcv_base_pos;
        short new_rcv_base_seq;

        //
        boolean lastPacketReceived = false;

        // === Start receiving packets ============================
        System.out.println("Waiting for data now.");
        while (true) {
            DatagramPacket received_packet = new DatagramPacket(new byte[header_length + max_payload_length], header_length + max_payload_length);
            try {
                if (debug_level > 0) System.out.println("Waiting to receive packet.");
                receiver_socket.receive(received_packet);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("I/OError occurred while receiving a packet.");
                quit();
            }

            receivedSeq = (short)((received_packet.getData()[1] << 8) | received_packet.getData()[2] & 0xff);

            // DEBUG:
            // If this if-statement fails, Sender2b has done something wrong
            short baseSeq = window_packets.get(1).sequenceNumber;
            if (!checkWithinWindow(baseSeq, receivedSeq) && !checkIfRecentDuplicate(baseSeq, receivedSeq)) {
                System.out.println(receivedSeq + "???????");
                break;
            }
            printDebug(baseSeq, receivedSeq);

            // === Check if packet from before rcv_base; re-acknowledge if so ==========================================
            if (checkIfRecentDuplicate(baseSeq, receivedSeq)) {
                ack_data[0] = received_packet.getData()[0];
                ack_data[1] = received_packet.getData()[1];
                ack_data[2] = received_packet.getData()[2];
                try {
                    DatagramPacket ack = new DatagramPacket(ack_data, ack_data.length, received_packet.getAddress(), received_packet.getPort());
                    receiver_socket.send(ack);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("Error sending ACK.");
                    quit();
                }
                continue;
            }

            // Check last-bit field
            if (received_packet.getData()[0] == 1) {
                lastPacketReceived = true;
            }

            if (debug_level > 0) System.out.println("\nPacket received; seqNum: " + receivedSeq);

            if (window_packets.get(receivedSeq) == null) {
                System.out.println("??????????");
                quit();
            }

            // === PACKET RECEIVED =====================================================================================

            // Mark packet received, copy its data into its PacketTracker
            window_packets.get(receivedSeq).received = true;
            System.arraycopy(received_packet.getData(), header_length, window_packets.get(receivedSeq).data, 0, received_packet.getLength() - header_length);

            // If received packet isn't rcv_base, no need to shift window
            if (receivedSeq != window_packets.keySet().iterator().next()) continue;

            // Else, shift the window by finding the first unacknowledged packet in the window, removing all
            // packets from before it, and updating remaining packets' window positions to reflect the window shift
            window_packets.remove(receivedSeq);
            new_rcv_base_pos = 2;
            new_rcv_base_seq = incrementSequenceNumber(receivedSeq);
            for (int i = 2; i <= window_packets.size(); i++) {
                if (window_packets.get(new_rcv_base_seq).received) {
                    window_packets.remove(new_rcv_base_seq);

                    new_rcv_base_pos++;
                    new_rcv_base_seq = incrementSequenceNumber(new_rcv_base_seq);
                } else break;
            }
            for (short seqNum : window_packets.keySet()) {
                window_packets.get(seqNum).windowPosition -= (new_rcv_base_pos - 1);
            }

            int num_packets_to_add;


        }

        writeFile();

        System.out.println("\nAll data has been received.");
        //System.out.println(rcv_base + " packets were received in total.");
        System.out.println("Received file is " + received_data.length + " bytes.");
        System.out.println(num_duplicates + " duplicate/out of order packets were received in total.");

        for (int i = 899169; i < received_data.length; i++) {
            if (received_data[i] != (byte)0) {
                System.out.println("non-zero extra bits");
                break;
            }
        }

        System.out.println(received_data.length - 899169);


    }

    /** Checks whether an incoming sequence number is within the receiver's current window.
     * @param baseSeq: current rcv_base
     * @param seqNum: sequence number to test
     * @return
     */
    private static boolean checkWithinWindow(short baseSeq, short seqNum) {
        short nextseq = baseSeq;
        for (int i = 0; i < window_size; i++) {
            if (nextseq == seqNum) return true;
            else nextseq = incrementSequenceNumber(nextseq);
        }
        return false;
    }

    /**
     * Checks whether an incoming sequence number is a duplicate whose sequence number is at most window_size
     * less than the current rcv_base sequence number.
     * @param baseSeq
     * @param seqNum
     * @return
     */
    private static boolean checkIfRecentDuplicate(short baseSeq, short seqNum) {
        short nextseq = decrementSequenceNumber(baseSeq);
        for (int i = 1; i < window_size; i++) {
            if (nextseq == seqNum) return true;
            else nextseq = decrementSequenceNumber(nextseq);
        }
        return false;
    }

    private static short incrementSequenceNumber(short seqNum) {
        int overflowed = (int) seqNum + 1;
        short nextseqnum = (overflowed > 32767) ? 1 : (short)(overflowed);
        return nextseqnum;
    }

    private static short decrementSequenceNumber(short seqNum) {
        int overflowed = (int) seqNum - 1;
        short ret = (overflowed == 0) ? 32767 : (short) overflowed;
        return ret;
    }

    /**
     * Closes socket (if it exists) and exits.
     */
    private static void quit() {
        try {
            receiver_socket.close();
            System.exit(0);
        } catch (NullPointerException e) {
            System.exit(0);
        }
    }

    private static void writeFile() {
        try {
            File outputImage = new File(file_to_write);
            OutputStream out = new FileOutputStream(outputImage);
            out.write(received_data);
            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error finding .jpg file to write to.");
            quit();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("I/OError occurred writing image file.");
            quit();
        }
    }

    /**
     * 0: no debug info. 1: print every received packet's info. 2: print only duplicate/out of order packets' info
     * @param expSeq
     * @param rcvSeq
     */
    private static void printDebug(short expSeq, short rcvSeq) {

        switch (debug_level) {
            case 0:
                break;
            case 1:
                System.out.println("==================================================");
                if (expSeq != rcvSeq) System.out.println("DUPLICATE/OUT OF ORDER RECEIVED:");
                else System.out.println("NEW PACKET RECEIVED:");
                System.out.println("Exp seq: " + expSeq);
                System.out.println("Rcv seq: " + rcvSeq);
                break;
            case 2:
                if (expSeq == rcvSeq) break;
                System.out.println("==================================================");
                System.out.println("DUPLICATE/OUT OF ORDER RECEIVED:");
                System.out.println("Exp seq: " + expSeq);
                System.out.println("Rcv seq: " + rcvSeq);
                break;
        }


        if (map_debug) {
            System.out.println("\nPACKETS IN BUFFER (OUT-OF-ORDER RECEIVED PACKETS):");
            for (short s : tempBuffer.keySet()) {
                byte[] d = tempBuffer.get(s);
                short seq = (short) ((d[1] << 8) | d[2] & 0xff);
                System.out.println(seq + " ==> " + seqToPos.get(seq));
            }

            System.out.println("\nSEQ TO POS MAP:");
            for (short seq : seqToPos.keySet()) {
                System.out.println(seq + " ==> " + seqToPos.get(seq));
            }
        }

    }

    private static class PacketTracker {

        byte[] data;
        short sequenceNumber;
        int windowPosition;
        boolean received;

        PacketTracker(short sequenceNumber, int windowPosition) {
            this.sequenceNumber = sequenceNumber;
            this.windowPosition = windowPosition;
            this.received = false;
        }


    }

}
