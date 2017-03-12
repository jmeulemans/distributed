import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import java.io.*;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ComputeNode {
    protected ComputeNodeServerHandler handler;
    protected String ip;
    protected int port;
    protected static ServerClient ServerClient;
    protected static String serverIP;
    protected static int serverPort;
    protected static TMultiplexedProcessor processor;


    private Map<Integer, Thread> runningProcesses;

    public ComputeNode(String ip, int port){
        runningProcesses = new HashMap<>();
        this.ip = ip;
        this.port = port;
    }

    public static void main(String[] args)
    {
        org.apache.log4j.BasicConfigurator.configure();
        int port = Integer.parseInt(args[0]);
        if(args.length == 2)
        {

        }
        org.apache.log4j.BasicConfigurator.configure();

        try {
            String ip = InetAddress.getLocalHost().getHostAddress();
            parseCoordinator("server_list.txt");
            ComputeNode node = new ComputeNode(ip,port);
            processor = new TMultiplexedProcessor();
            TServerTransport serverTransport = new TServerSocket(node.port);
            node.handler = new ComputeNodeServerHandler(node);
            processor.registerProcessor("ServerToNodeService", new ServerToNodeService.Processor(node.handler));
            TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport)
                    .processor(processor);
            TServer server = new TThreadPoolServer(serverArgs);
            Runnable registerRunnable = new Runnable() {
                public void run(){
                    try{
                        Float failProb = null;
                        if(args.length == 2)
                        {
                            failProb = Float.parseFloat(args[1]);
                        }
                        registerRunnable(node, failProb);
                    } catch (Exception e){
                        e.printStackTrace();
                    }


                }
            };
            Thread registerThread = new Thread(registerRunnable);
            registerThread.start();
            System.out.println("Serving");
            server.serve();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    private static void registerRunnable(ComputeNode node, Float failProb)  {
        ServerClient = new ServerClient();
        try {
            String result  = ServerClient.registerNode(node.ip, node.port);
            String[] id_failProb = result.split(":");
            int id = Integer.parseInt(id_failProb[0]);
            Float fail = failProb != null ? failProb : Float.parseFloat(id_failProb[1]);
            System.out.println("Registered with server: "+result);
            node.handler.failProb = fail;
            node.handler.id = id;
        } catch (TException e) {
            System.out.println(e);
        }

    }

    private static void parseCoordinator(String filename) throws IOException {
        Optional<String> lineOption = Files.lines(Paths.get(filename)).findFirst();
        String line = lineOption.get();
        serverIP = line.split(":")[0];
        serverPort = Integer.parseInt(line.split(":")[1]);
    }
    static class ComputeNodeServerHandler implements ServerToNodeService.Iface{
        public ComputeNode node;
        public float failProb;
        public int id;
        protected volatile float totalJobTime;
        protected volatile int numberOfJobs = 0;

        public ComputeNodeServerHandler(ComputeNode node){
            this.node = node;
        }

        @Override
        public String sort(String filename, int offset, int len, int id) throws TException {
            long start = System.currentTimeMillis();
            numberOfJobs++;
            final String[] sortedFile = new String[1];
            Thread t = new Thread(() -> {
                try {
                    FileInputStream file = new FileInputStream(filename);
                    byte[] readResults = new byte[len];
                    file.skip(offset);
                    file.read(readResults, 0, len);
                    String outFileName =  id+".sort";
                    System.out.println("Sorting bytes "+offset+" - "+ (offset+len)+" of "+filename+"->"+outFileName);
                    FileOutputStream fw = new FileOutputStream(outFileName);
                    Arrays.asList(new String(readResults).split(" "))
                            .stream()
                            .map(s -> Integer.parseInt(s.trim()))
                            .sorted().forEach( i ->
                                    {
                                        try {
                                            fw.write((i + " ").getBytes());
                                        } catch (IOException e) {
                                            System.out.println("Error occurred in sort writes");
                                            e.printStackTrace();
                                        }
                                    }
                            );
                    fw.flush();
                    try
                    {
                        fw.getFD().sync();
                        fw.close();
                        sortedFile[0] = outFileName;
                    }
                    catch(SyncFailedException e)
                    {
                        sortedFile[0] = null;
                    }

                }
                catch(Exception e){
                    sortedFile[0] = null;
                }
            });
            node.runningProcesses.put(id, t);
            try {
                t.start();
                t.join();
                if(node.runningProcesses.containsKey(id)) {
                    node.runningProcesses.remove(id);
                }
                totalJobTime += ((float)(System.currentTimeMillis()-start))/1000.;
                return sortedFile[0];
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }

        }
        private class MergeTuple implements Comparable
        {
            //wrapper for file scanner that can be added to a priority queue
            Scanner ints;
            Integer current;

            MergeTuple(String filename)
            {
                try {
                    // wait for the file to exist, should be immediately available due to syncs
                    Path path = Paths.get(filename);
                    while(!Files.exists(path))
                    {
                        try {
                            Thread.currentThread().sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    ints = new Scanner(new File(filename));
                    if(ints.hasNextInt())
                    {
                        current = ints.nextInt();
                    }
                    else
                    {
                        current = null;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            Integer getNextInt()
            {
                Integer temp = current;
                if(ints != null && ints.hasNextInt())
                {
                    current = ints.nextInt();
                    return temp;
                }
                current = null;
                return temp;
            }
            @Override
            public String toString()
            {
                return "("+current+", "+ints.hasNextInt()+")";
            }

            @Override
            public int compareTo(Object o) {
                //insure that null is the max value, putting such merge tuples at end of priority queue
                if(o instanceof MergeTuple)
                {
                    Integer l = this.current;
                    Integer r =((MergeTuple) o).current;
                    if(l!=null && r != null) {
                        return Integer.compare(l, r);
                    }
                    if(l==null && r==null)
                    {
                        return 0;
                    }
                    return l==null ? 1 : -1;
                }
                return -1;
            }
        }

        @Override
        public String merge(List<String> filenames, int id) throws TException {
            long start = System.currentTimeMillis();
            numberOfJobs++;
            final String[] sortedFile = new String[1];
            Thread t = new Thread(() -> {
                PriorityQueue<MergeTuple> q = new PriorityQueue<>();
                for (String filename : filenames) {
                    q.add(new MergeTuple(filename));
                }
                String outFileName = id + ".sort";
                System.out.println("Merging " + filenames + "->" + outFileName);
                try {
                    FileOutputStream fw = new FileOutputStream(outFileName);
                    //continually write min values of the top of each file, advance to next number from that file
                    while (!q.isEmpty()) {
                        MergeTuple topTuple = q.poll();
                        Integer top = topTuple.getNextInt();
                        if (top != null) {
                            fw.write((top + " ").getBytes());
                            q.add(topTuple);
                        }
                    }
                    fw.flush();
                    try {
                        fw.getFD().sync();
                        fw.close();
                        sortedFile[0] = outFileName;
                    }
                    catch(SyncFailedException e)
                    {
                        sortedFile[0] = null;
                    }

                } catch (Exception e) {
                    sortedFile[0] = null;
                }
            });
            node.runningProcesses.put(id, t);
            try {
                t.start();
                t.join();
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
                sortedFile[0] = null;
            }
            finally {
                if(node.runningProcesses.containsKey(id)) {
                    node.runningProcesses.remove(id);
                }

            }
            totalJobTime += ((float)(System.currentTimeMillis()-start))/1000.;
            return sortedFile[0];
        }

        @Override
        public boolean kill(int id) throws TException {
            if(node.runningProcesses.containsKey(id)) {
                Thread t = node.runningProcesses.get(id);
                node.runningProcesses.remove(id);
                t.interrupt();
                return true;
            }
            return false;
        }

        @Override
        public void notifyOfJob() throws TException {
            double rand = Math.random();
            // check if this node will fail, if it will, fail randomly in the next 10s
            if(rand < failProb)
            {
                System.out.println("Node failure imminent");
                rand = Math.random();
                double timeToFail = rand * 10;

                    Runnable killRunnable = new Runnable() {
                        public void run(){
                            try{
                                Thread newThread = Thread.currentThread();
                                newThread.sleep((long) Math.floor(timeToFail*1000));
                            } catch (Exception e){
                                e.printStackTrace();
                            }
                            finally
                            {
                                System.exit(1);
                            }

                        }
                    };

                    Thread killerThread = new Thread(killRunnable);
                    killerThread.start();
                }
            }

        @Override
        public boolean setParameter(KeyValue parameter) throws TException {
            if("failprob".equals(parameter.getName()))
            {
                Float value = Float.parseFloat(parameter.getValue());
                if(value>=0 && value <= 1)
                {
                    failProb = value;
                    return true;
                }
            }
            return false;
        }

        @Override
        public String getInfoFromNode() throws TException {
            return numberOfJobs+" tasks at " +totalJobTime/numberOfJobs+" s/task";
        }

        @Override
        public boolean ping() throws TException {
            return true;
        }
    }
    static class ServerClient implements NodeToServerService.Iface
    {
        private static TTransport transport;
        private static NodeToServerService.Client client;

        public ServerClient(){
            try {
                transport = new TSocket(serverIP, serverPort);
                TProtocol protocol = new TBinaryProtocol(transport);
                //TODO: may be issue with unopened transport here
                TMultiplexedProtocol coordProtocol = new TMultiplexedProtocol(protocol, "NodeToServerService");
                client = new NodeToServerService.Client(coordProtocol);
            } catch (Exception x) {
                x.printStackTrace();
            }
        }
        @Override
        public String registerNode(String ip, int port) throws TException {
            transport.open();
            String result = client.registerNode(ip, port);
            transport.close();
            return result;
        }
    }
}