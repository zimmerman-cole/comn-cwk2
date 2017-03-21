import java.net.*;
import java.io.*;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.HashMap;

public class Receiver2b
{
    private static final int header_length = 3;
    private static final int max_payload_length = 1024;
    private static final int debug_level = 0;
    private static final boolean map_debug = true;

    private static DatagramSocket receiver_socket;
    private static String file_to_write;
    private static byte[] received_data = new byte[0];

    private static HashMap<Short, byte[]> tempBuffer;
    private static LinkedHashMap<Short, Short> seqToPos;


    public static void main(String[] args) {

        int num_duplicates = 0;

        int portNumber = Integer.parseInt(args[0]);
        file_to_write = args[1];
        short window_size = (short) Integer.parseInt(args[2]);
        if (window_size > 16383 || window_size <= 0) {
            System.out.println("Please enter a window size N such that 0 < N <= 16383.");
            System.exit(0);
        }
        tempBuffer = new HashMap<Short, byte[]>(window_size);
        try {
            receiver_socket = new DatagramSocket(portNumber);
        } catch (SocketException e) {
            e.printStackTrace();
            System.out.println("SocketException occurred while initializing receiver socket (port # already in use?).");
            quit();
        }

        // Last-message byte is stored in position 0 in the header.
        // Sequence numbers for this part are stored in packet.getData()[1,2]
        // Sequence numbers are in range [0, 32767] (using 0-based indexing)
        short expectedseqnum = 0;
        int rcv_base = 0;
        byte[] ack_data = new byte[6];
        System.arraycopy("ACK".getBytes(), 0, ack_data, header_length, 3);

        boolean lastPacketReceived = false;
        short lastPacketSeqNum = -1;

        // Initialize maps:
        seqToPos = new LinkedHashMap<Short, Short>(window_size);
        for (short i = 0; i < window_size; i++) {
            seqToPos.put(i, i);
        }

        // === Start receiving packets ============================
        System.out.println("Waiting for data now.");
        while (true) {
            DatagramPacket received_packet = new DatagramPacket(new byte[header_length + max_payload_length], header_length + max_payload_length);
            try {
                receiver_socket.receive(received_packet);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("I/OError occurred while receiving a packet.");
                quit();
            }

            short receivedSeq = (short)((received_packet.getData()[1] << 8) | received_packet.getData()[2] & 0xff);
            printDebug(rcv_base, expectedseqnum, receivedSeq);

            // === Check packet isn't from before rcv_base; re-acknowledge if so =======================================
            try {
                seqToPos.get(receivedSeq);
            } catch (NullPointerException e) {
                // Send ACK:
                ack_data[0] = received_packet.getData()[0]; ack_data[1] = received_packet.getData()[1]; ack_data[2] = received_packet.getData()[2];
                DatagramPacket ack = new DatagramPacket(ack_data, ack_data.length, received_packet.getAddress(), received_packet.getPort());
                try {
                    receiver_socket.send(ack);
                } catch (IOException ex) {
                    e.printStackTrace();
                    System.out.println("I/OError occurred while sending ACK.");
                    quit();
                }
                continue;
            }

            // === Check last-bit field ================================================================================
            if (received_packet.getData()[0] == 1) {
                lastPacketReceived = true;
                lastPacketSeqNum = receivedSeq;
            }

            // =========================================================================================================
            // === rcv_base packet received; store temporary buffer and update window, etc. ============================
            // =========================================================================================================
            if (receivedSeq == expectedseqnum) {
                // Store packet data (including header) into first slot of temporary buffer
                tempBuffer.put((short) 0, received_packet.getData());

                // To keep track of where the receiver's new base will be.
                short new_rcv_base_pos = window_size;
                // Add each packet's data (not including header) to received_data.
                for (short i = 0; i < window_size; i++) {
                    byte[] pck_data;
                    try {
                        pck_data = tempBuffer.get(i);
                    } catch (NullPointerException e) {
                        new_rcv_base_pos = i;
                        break;
                    }

                    byte[] tmp = new byte[received_data.length + pck_data.length - header_length];
                    System.arraycopy(received_data, 0, tmp, 0, received_data.length);
                    System.arraycopy(pck_data, header_length, tmp, received_data.length, pck_data.length - header_length);
                    received_data = tmp;
                }

                // Send ACK:
                ack_data[0] = received_packet.getData()[0]; ack_data[1] = received_packet.getData()[1]; ack_data[2] = received_packet.getData()[2];
                DatagramPacket ack = new DatagramPacket(ack_data, ack_data.length, received_packet.getAddress(), received_packet.getPort());
                try {
                    receiver_socket.send(ack);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("I/OError occurred while sending ACK.");
                    quit();
                }

                // If the last packet has been received, check if all packets in window before it have also been received,
                // and break if true.
                boolean allReceived = true;
                if (lastPacketReceived) {
                    short lastPacketPos = seqToPos.get(lastPacketSeqNum);

                    for (short i = 0; i < lastPacketPos; i++) {
                        try {
                            byte[] d = tempBuffer.get(i);
                        } catch (NullPointerException e) {
                            allReceived = false;
                            break;
                        }
                    }
                    if (allReceived) break;
                }

                // Update rcv_base, (seqNum => windowPosition) map, expectedseqnum
                rcv_base += new_rcv_base_pos;
                int expseq_overflowed = (int) expectedseqnum + new_rcv_base_pos;
                expectedseqnum = (expseq_overflowed > 32767) ? (short)(expseq_overflowed - 32767 - 1) : (short) expseq_overflowed;
                seqToPos.clear();

                for (int i = 0; i < window_size; i++) {
                    int overflowedseqnum = expseq_overflowed + i;
                    short seqnum = (overflowedseqnum > 32767) ? (short)(overflowedseqnum - 32767 - 1) : (short)(overflowedseqnum);
                    seqToPos.put(seqnum, (short)i);
                }

                // Update temporary buffer
                HashMap<Short, byte[]> oldBuffer = tempBuffer;
                tempBuffer.clear();
                Iterator<Short> keySet = oldBuffer.keySet().iterator();
                while (keySet.hasNext()) {
                    short key = keySet.next();
                    short newKey = (short)(key - new_rcv_base_pos);
                    if (newKey >= 0) tempBuffer.put(newKey, oldBuffer.get(key));
                }

            // =========================================================================================================
            // === Packet is out of order; acknowledge and store it in temporary buffer (if not already done) ==========
            // =========================================================================================================
            } else {
                Short receivedPos = seqToPos.get(receivedSeq);
                byte[] data = received_packet.getData();
                if (receivedPos != null) {
                    try {
                        data = tempBuffer.get(receivedPos);
                    } catch (NullPointerException e) {
                        tempBuffer.put(receivedPos, data);
                    }
                }

                // Send ACK:
                ack_data[0] = data[0]; ack_data[1] = data[1]; ack_data[2] = data[2];
                DatagramPacket ack = new DatagramPacket(ack_data, ack_data.length, received_packet.getAddress(), received_packet.getPort());
                try {
                    receiver_socket.send(ack);
                } catch (IOException ex) {
                    ex.printStackTrace();
                    System.out.println("I/OError occurred while sending ACK.");
                    quit();
                }
            }

        }

        writeFile();

        System.out.println(rcv_base + " packets were received in total.");
        System.out.println("Received file is " + received_data.length + " bytes.");
        System.out.println(num_duplicates + " duplicate/out of order packets were received in total.");


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
     * @param numRcv
     * @param expSeq
     * @param rcvSeq
     */
    private static void printDebug(int numRcv, short expSeq, short rcvSeq) {

        switch (debug_level) {
            case 0:
                break;
            case 1:
                System.out.println("==================================================");
                if (expSeq != rcvSeq) System.out.println("DUPLICATE/OUT OF ORDER RECEIVED:");
                System.out.println("Exp abs: " + numRcv);
                System.out.println("Exp seq: " + expSeq);
                System.out.println("Rcv seq: " + rcvSeq);
                break;
            case 2:
                if (expSeq == rcvSeq) break;
                System.out.println("==================================================");
                System.out.println("DUPLICATE/OUT OF ORDER RECEIVED:");
                System.out.println("Exp abs: " + numRcv);
                System.out.println("Exp seq: " + expSeq);
                System.out.println("Rcv seq: " + rcvSeq);
                break;
        }


        if (map_debug) {
            System.out.println("\nWINDOW_POS TO BYTE[] BUFFER:");
            for (short s : tempBuffer.keySet()) {
                byte[] d = tempBuffer.get(s);
                short seq = (short) ((d[1] << 8) | d[2] & 0xff);
                System.out.println(s + " ==> seq: " + seq);
            }

            System.out.println("\nSEQ TO POS MAP:");
            for (short seq : seqToPos.keySet()) {
                System.out.println(seq + " ==> " + seqToPos.get(seq));
            }
        }

    }

}
