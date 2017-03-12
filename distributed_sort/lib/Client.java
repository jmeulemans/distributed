import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

public class Client {
    public static Scanner in;
    //    public static List<String> testFiles;
    public static ServerClient server;

    //TODO: wrap read and write of ServerClient
    //TODO: UI in main method
    public static void main(String[] args) {
//        if (testFiles == null) {
//            testFiles = new ArrayList<>();
//            for (int i = 0; i < 100; i++) {
//                testFiles.add("file" + i);
//            }
//        }
        in = new Scanner(System.in);
        if (args.length == 1) {
            try {
                parseServers(args[0]);
            } catch (Exception e) {
                System.out.println(e.getStackTrace());
                return;
            }

            boolean readingInput = true;
            System.out.print("Enter a command (-h for help): ");

            while (readingInput) {
                String userInput = in.nextLine();
                String[] userArgs = userInput.split(",");
                if (userArgs.length > 0) {
                    if (userArgs[0].equals("-h")) {
                        printHelp();
                    } else if (userArgs[0].equals("sort")) {
                        try {
                            long start = System.currentTimeMillis();
                            System.out.println("Sorted_file: " + server.sort(userArgs[1]));
                            System.out.println("Task took "+((float)(System.currentTimeMillis()-start))/1000. +" seconds");
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    }
                    else if(userArgs[0].equals("info"))
                    {
                        try {
                            System.out.print(server.getInfoFromServer());
                        } catch (TException e) {
                            System.out.println("Unable to retrieve info from server");
                        }
                    }
                    else if(userArgs.length == 3 && userArgs[0].equals("set"))
                    {
                        KeyValue keyValue = new KeyValue(userArgs[1], userArgs[2]);
                        try {
                            if(server.setParameter(keyValue))
                            {
                                System.out.println("Parameter "+userArgs[1]+" set to "+userArgs[2]);
                            }
                            else
                            {
                                System.out.println("Parameter "+userArgs[1]+" could not be set to "+userArgs[2]);
                            }
                        } catch (TException e) {
                            e.printStackTrace();
                        }
                    }
                    else{
                        System.out.println("Invalid command.");
                        printHelp();
                    }
                }
            }

        }
    }

    private static void parseServers(String filename) throws IOException {
        List<ServerClient> servers = new ArrayList<>();
        Files.lines(Paths.get(filename)).forEach(line -> servers.add(new ServerClient(line.split(":")[0],
                Integer.parseInt(line.split(":")[1]))));
        server = servers.get(0);

    }


    private static void printHelp() {
        System.out.println("sort,<filename>");
        System.out.println("info");
        System.out.println("set,<chunksize, nodespertask, taskspermerge, failprob>,value");
    }


    public static class ServerClient implements ClientToServerService.Iface {
        private String ip;
        private int port;
        private TTransport transport;
        private ClientToServerService.Client client;

        public ServerClient(String ip, int port) {
            try {
                transport = new TSocket(ip, port);
                System.out.println(ip + ":" + port);
                this.ip = ip;
                this.port = port;
                TProtocol protocol = new TBinaryProtocol(transport);
                //TODO: may be issue with unopened transport here
                TMultiplexedProtocol coordProtocol = new TMultiplexedProtocol(protocol, "ClientToServerService");
                client = new ClientToServerService.Client(coordProtocol);
                transport.open();
            } catch (Exception x) {
                x.printStackTrace();
            }

        }

        public String sort(String filename) throws TException {
            return client.sort(filename);
        }

        @Override
        public String getInfoFromServer() throws TException {
            return client.getInfoFromServer();
        }

        @Override
        public boolean setParameter(KeyValue parameter) throws TException {
            return client.setParameter(parameter);
        }

    }


}