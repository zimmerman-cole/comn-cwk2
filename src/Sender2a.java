import java.io.*;
import java.net.*;
import java.lang.Math;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;

public class Sender2a
{
    private static final int header_length = 3;
    private static final int max_payload_length = 1024;

    // DEBUG OPTIONS:
    private static final boolean debug = false;
    private static final boolean map_debug = false;
    private static final boolean retransmit_debug = false;
    private static final boolean promptEachTime = false;
    // ===============

    private static DatagramSocket sender_socket;
    private static String filename;
    private static byte[] data;
    private static byte[][] data_packets;
    private static int num_retransmissions = 0;
    private static long start_time;

    public static void main(String[] args) {
        // === Parse command-line arguments ================================
        String receiverHostname = args[0];
        int receiverPortNumber = Integer.parseInt(args[1]);
        filename = args[2];
        int timeout = Integer.parseInt(args[3]);
        short window_size = (short) Integer.parseInt(args[4]);
        if (window_size <= 0 || window_size >= 32767) {
            System.out.println("Please enter a window size N such that 0 < N < 32767.");
            System.exit(0);
        }
        try {
            sender_socket = new DatagramSocket();
            sender_socket.setSoTimeout(timeout);
        } catch (SocketException e) {
            e.printStackTrace();
            System.out.println("SocketException occurred while initializing sender socket (port # already in use?).");
            quit();
        }

        load_file();

        // === Start sending the data =====================================
        System.out.println("Sending data now.");

        // Sequence numbers are in range [1, 32767]
        short nextseqnum = 1;
        short baseseqnum = 1;
        // Base contains the absolute position of a packet (as sequence numbers overflow), uses 0-based indexing
        int base = 0;

        // Contains the window position of the next packet to be sent (1-based indexing)
        short nextseqpos = 1;

        // Maps packets' absolute positions to their positions in the window.
        LinkedHashMap<Integer, Short> absToPositionMap = new LinkedHashMap<Integer, Short>((int)window_size);
        // Maps packet's sequence numbers to their positions in the window.
        LinkedHashMap<Short, Short> seqToPositionMap = new LinkedHashMap<Short, Short>((int)window_size);

        try {
            InetAddress receiver_address = InetAddress.getByName(receiverHostname);
            // This point is recorded as the start of transmission
            start_time = System.currentTimeMillis();

            // Loops through each packet to be sent
            while (true) {

                // === FOR DEBUGGING: ==================================================================================
                if (promptEachTime) {
                    try {
                        new BufferedReader(new InputStreamReader(System.in)).readLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                if (debug) System.out.println("==========================================================");
                if (debug) System.out.println("Window size: " + window_size);
                if (debug) System.out.println("baseseqnum: " + baseseqnum + ". nextseqnum: " + nextseqnum + ". nextseqpos: " + nextseqpos + ".\n");

                if (map_debug) {
                    System.out.println("ABSOLUTES TO SEQNUM POSITIONS:");
                    for (Object ab : absToPositionMap.keySet().toArray()) {
                        System.out.println(ab + " ==> " + absToPositionMap.get(ab) + ".");
                    }
                    System.out.println("SEQNUMS TO SEQNUM POSITIONS:");
                    for (Object seq : seqToPositionMap.keySet().toArray()) {
                        System.out.println(seq + " ==> " + seqToPositionMap.get(seq) + ".");
                    }
                    System.out.println();
                }
                // === /FOR DEBUGGING: =================================================================================

                // Contains the absolute packet number of the next packet to be sent (0-based indexing)
                int absolutePckNum = base + nextseqpos - 1;

                // =====================================================================================================
                // === Fill the pipeline if there's space left in window ===============================================
                // =====================================================================================================
                if (absolutePckNum != data_packets.length && nextseqpos <= window_size) {
                    // Assemble the next packet.
                    byte[] pck_data = new byte[header_length + data_packets[absolutePckNum].length];
                    System.arraycopy(data_packets[absolutePckNum], 0, pck_data, header_length, data_packets[absolutePckNum].length);

                    // Assign the packet its sequence number.
                    pck_data[1] = (byte) ((nextseqnum >> 8) & 0xff);
                    pck_data[2] = (byte) (nextseqnum & 0xff);
                    // Set last-message bit if applicable
                    if (absolutePckNum == data_packets.length - 1) pck_data[0] = 1;


                    DatagramPacket sendPacket = new DatagramPacket(pck_data, pck_data.length, receiver_address, receiverPortNumber);
                    try {
                        sender_socket.send(sendPacket);
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.out.println("IO/Error sending packet w/ seqNum: " + nextseqnum);
                        quit();
                    }

                    if (debug) System.out.println("Sending packet, sequence number: " + nextseqnum + ", position: " + nextseqpos + ".");

                    // Store this packet's relative window position
                    absToPositionMap.put(absolutePckNum, nextseqpos);
                    seqToPositionMap.put(nextseqnum, nextseqpos);

                    // Increment nextseqnum and its relative position
                    nextseqnum = (nextseqnum == 32767) ? 1 : (short)(nextseqnum + 1);
                    nextseqpos++;
                    if (debug) System.out.println("==========================================================");
                    continue;
                }

                // =====================================================================================================
                // === Window is full; wait for ACK ====================================================================
                // =====================================================================================================
                DatagramPacket rcvPacket = new DatagramPacket(new byte[6], 6);
                try {
                    sender_socket.receive(rcvPacket);
                } catch (SocketTimeoutException e) {
                    // === TIMEOUT: resend all sent packets in window ==================================================
                    if (retransmit_debug) System.out.println("Retransmitting from baseseqnum: " + baseseqnum + ".");
                    num_retransmissions++;

                    for (int i = base; i < absolutePckNum; i++) {

                        byte[] pck_data = new byte[header_length + data_packets[i].length];
                        System.arraycopy(data_packets[i], 0, pck_data, header_length, data_packets[i].length);

                        // Get packet's sequence number (handle overflows as well)
                        int overflowedseqnum = (int) baseseqnum + (int) absToPositionMap.get(i) - 1;
                        short seqnum = (overflowedseqnum > 32767) ? (short)(overflowedseqnum - 32767) : (short) overflowedseqnum;


                        pck_data[1] = (byte) ((seqnum >> 8) & 0xff);
                        pck_data[2] = (byte) (seqnum & 0xff);
                        if (i == data_packets.length - 1) pck_data[0] = 1;
                        DatagramPacket sendPacket = new DatagramPacket(pck_data, pck_data.length, receiver_address, receiverPortNumber);

                        try {
                            sender_socket.send(sendPacket);
                        } catch (IOException ex) {
                            ex.printStackTrace();
                            System.out.println("IO/Error sending packet, sequence number: " + seqnum + ".");
                        }
                    }
                    if (debug) System.out.println("==========================================================");
                    continue;
                    // =================================================================================================

                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("IO/Error receiving ACK for packet, nextseqnum: " + nextseqnum + ".");
                    quit();
                }

                // =====================================================================================================
                // === ACK received; update sequence numbers, positions, etc. ==========================================
                // =====================================================================================================

                // Exit loop upon receipt of last packet:
                if (rcvPacket.getData()[0] == 1) {
                    System.out.println("Last packet's ACK received.");
                    break;
                }

                short receivedSeq = (short) ((rcvPacket.getData()[1] << 8) | rcvPacket.getData()[2] & 0xff);
                Short receivedPos = seqToPositionMap.get(receivedSeq);
                // Ignore ACKS from already-acknowledged packets
                if (receivedPos == null) continue;
                int receivedAbs = base + receivedPos - 1;

                if (debug) System.out.println("ACK received for packet, sequence number: " + receivedSeq + ", position: " + receivedPos + ".");

                // Remove (abs => seqPos) mappings for newly accounted-for packets
                for (int i = base; i <= receivedAbs; i++) {
                    absToPositionMap.remove(i);
                }
                // Remove (seq => seqPos) mappings for newly accounted-for packets
                for (int i = 1; i <= receivedPos; i++) {
                    int overflowedseqnum = (int) baseseqnum + i - 1;
                    short seqnum = (overflowedseqnum > 32767) ? (short) (overflowedseqnum - 32767) : (short) overflowedseqnum;
                    seqToPositionMap.remove(seqnum);
                }

                // Decrement the position values for the remaining mappings to reflect the window shift
                for (int key : absToPositionMap.keySet()) {
                    absToPositionMap.put(key, (short)(absToPositionMap.get(key) - receivedPos - 1));
                }
                for (short key : seqToPositionMap.keySet()) {
                    seqToPositionMap.put(key, (short)(seqToPositionMap.get(key) - receivedPos - 1));
                }

                // Update base, baseseqnum, nextseqnum's window position
                nextseqpos = (short) (nextseqpos - receivedPos);
                base = receivedAbs + 1;
                int overflowed_baseseq = (int) baseseqnum + 1;
                baseseqnum = (overflowed_baseseq > 32767) ? 1 : (short) (receivedSeq + 1);

                if (debug) System.out.println("==========================================================");
            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.out.println("Error retrieving receiver's IP address.");
            quit();
        }

        double trans_time = (System.currentTimeMillis() - start_time) / 1000.0;

        System.out.println("All data has been sent and accounted for.\n");
        System.out.println("There were " + num_retransmissions + " retransmissions in total.");
        System.out.println("Total transmission time: " + trans_time + " seconds.");
        System.out.println("Average throughput: " + (data_packets.length / trans_time) + " KB/s.");

    }



    /**
     * This method reads the specified file in 'filename' into byte array 'data', and then splits that into
     * packet-sized chunks and stores those in byte-array array 'data_packets'.
     */
    private static void load_file() {

        try {
            File file = new File(filename);
            data = new byte[(int) file.length()];
            FileInputStream fis = new FileInputStream(file);
            fis.read(data);
        } catch (FileNotFoundException e) {

        } catch (IOException e) {
            e.printStackTrace();
            quit();
        }

        try {
            File file = new File("/mnt/shared/" + filename);
            data = new byte[(int) file.length()];
            FileInputStream fis = new FileInputStream(file);
            fis.read(data);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("File was not found. Please specify its full path, or put the file in mnt/shared and pass only the filename as an argument.");
            quit();
        } catch (IOException e) {
            e.printStackTrace();
            quit();
        }

        // Break data down into packet-sized chunks (each with maximum size max_payload_length) and store
        // the chunks in data_packets (storing the same data twice, but whatever).
        data_packets = new byte[(int)Math.ceil(data.length / (float)max_payload_length)][max_payload_length];
        for (int packetNum = 0; packetNum < (data_packets.length - 1); packetNum++) {
            data_packets[packetNum] = Arrays.copyOfRange(data, packetNum*max_payload_length, max_payload_length*(packetNum+1));
        }
        // avoid ArrayIndexOutOfBoundsException, do last packet separately
        data_packets[data_packets.length-1] = Arrays.copyOfRange(data, max_payload_length*(data_packets.length-1), data.length);

        System.out.println("\nSender has loaded the file and is ready to send.");
        System.out.println("File size: " + data.length + " bytes, consisting of " + data_packets.length + " packets.\n");
    }

    /**
     * Closes socket (if it exists) and exits.
     */
    public static void quit() {
        try {
            sender_socket.close();
            System.exit(0);
        } catch (NullPointerException e) {
            System.exit(0);
        }
    }

}
