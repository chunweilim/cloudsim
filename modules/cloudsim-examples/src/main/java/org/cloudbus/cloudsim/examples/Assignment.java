package org.cloudbus.cloudsim.examples;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Assignment {
    private static final String CSV_SEPARATOR = " ";
    private static final int NUM_OF_HOSTS = 500;
    private static final int PE_MIPS = 10000;
    private static final int NUM_OF_PES = 8; // Number of processing core(s)
    private static final int RAM_SIZE = 66560; // in MB
    private static final int STORAGE_SIZE = 68157440; // in MB
    private static final int BW_SIZE = 10000; // using the default as from other examples

    public static void main(String[] args) throws Exception {
        Log.printLine("Starting Assignment...");

        // First step: Initialize the CloudSim package. It should be called
        // before creating any entities.
        int num_user = 1;   // number of cloud users
        Calendar calendar = Calendar.getInstance();
        boolean trace_flag = false;  // mean trace events

        // Initialize the CloudSim library
        CloudSim.init(num_user, calendar, trace_flag);

        // Second step: Create Datacenters
        @SuppressWarnings("unused")
        Datacenter datacenter = createDatacenter("CWMetaDatacenter");

        //Third step: Create Broker
        DatacenterBroker broker = createBroker("CWBroker");
        int brokerId = broker.getId();

        // Preparation for list of cloudlets
        List<List<Integer>> records = readRecordsFromCsv();

        //Fourth step: Create VMs and Cloudlets and send them to broker
        List<Cloudlet> cloudletList = createCloudletListsFromRecords(brokerId, records);
        List<Vm> vmList = createVms(brokerId, cloudletList.size(), 0);

        broker.submitVmList(vmList);
        broker.submitCloudletList(cloudletList);

        // Fifth step: Starts the simulation
        CloudSim.startSimulation();

        List<Cloudlet> newList = broker.getCloudletReceivedList();

        CloudSim.stopSimulation();

        // Final step: Print results when simulation is over
        printCloudletList(newList);

        Log.printLine("Assignment finished!");
    }

    /**
     * Create a datacenter with 500 hosts.
     * Each host contains:
     * 8 cores (10000 MIPs each)
     * 65 GB RAM
     * 10 TB storage
     * @param name
     * @return
     */
    private static Datacenter createDatacenter(String name) {

        List<Host> hostList = new ArrayList<>();
        IntStream.range(0, NUM_OF_HOSTS)
                .forEach(hostId -> {

                    List<Pe> peList = createPes();
                    hostList.add(
                            new Host(
                                    hostId,
                                    new RamProvisionerSimple(RAM_SIZE),
                                    new BwProvisionerSimple(BW_SIZE),
                                    STORAGE_SIZE,
                                    peList,
                                    new VmSchedulerSpaceShared(peList)
                            )
                    );
                });

        DatacenterCharacteristics datacenterCharacteristics = createDatacenterCharacteristics(hostList);

        Datacenter datacenter = null;
        try {
            LinkedList<Storage> storageList = new LinkedList<>();	//we are not adding SAN devices by now
            datacenter = new Datacenter(name, datacenterCharacteristics,
                    new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return datacenter;
    }

    private static List<Pe> createPes() {
        // Create the cores for each host according to spec
        List<Pe> peList = new ArrayList<>();

        IntStream.range(0, NUM_OF_PES)
                .forEach(id -> peList.add(new Pe(id, new PeProvisionerSimple(PE_MIPS))));

        return peList;
    }

    private static DatacenterCharacteristics createDatacenterCharacteristics(List<Host> hostList) {

         /*
         Create a DatacenterCharacteristics object that stores the
         properties of a data center: architecture, OS, list of
         Machines, allocation policy: time- or space-shared, time zone
         and its price (G$/Pe time unit).
        */
        String arch = "x86";      // system architecture
        String os = "Linux";          // operating system
        String vmm = "Xen";
        double time_zone = 10.0;         // time zone this resource located
        double cost = 3.0;              // the cost of using processing in this resource
        double costPerMem = 0.05;		// the cost of using memory in this resource
        double costPerStorage = 0.1;	// the cost of using storage in this resource
        double costPerBw = 0.1;			// the cost of using bw in this resource

        return new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);
    }

    // TODO: change here to have new broker?
    //We strongly encourage users to develop their own broker policies, to submit vms and cloudlets according
    //to the specific rules of the simulated scenario
    private static ScheduleBroker createBroker(String name) throws Exception {

        return new ScheduleBroker(name);
    }

    /**
     * Let's assume for now we create VM according to the number of cloudlets.
     * @param numberOfCloudlets
     * @return
     */
    public static List<Vm> createVms(int brokerId, int numberOfCloudlets, int vmIdShift) {

        List<Vm> vmList = new ArrayList<>();
        // Below specs are from examples as well
        int mips = 250;
        long size = 10000; //image size (MB)
        int ram = 2048; //vm memory (MB)
        long bw = 1000;
        int pesNumber = 1; //number of cpus
        String vmm = "Xen"; //VMM name

        IntStream.range(0, numberOfCloudlets)
                .forEach(vmId -> vmList.add(
                        new Vm(vmId + vmIdShift, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared())
                ));
        return vmList;
    }

    /**
     * Create a map of submission time to cloudlet.
     * Refer to CloudSimExample8 later on how to pause simulation to submit stuff?
     * @param records
     * @return
     */
    private static Map<Integer, List<Cloudlet>> createMapOfSubmissionTimeToCloudlets(int brokerId, List<List<Integer>> records) {

        Map<Integer, List<Cloudlet>> cloudletMap = new HashMap<>();

        UtilizationModel utilizationModel = new UtilizationModelFull();

        for (int i = 0; i < records.size(); i++) {
            List<Integer> currentRecord = records.get(i);

            // in whatever CloudSim uses as time unit (which I think is ms)
            int cloudletSubmissionTime = currentRecord.get(0);
            int cloudletMIs = currentRecord.get(1);
            // I am assuming min memory meaning the file size of the cloudlet has to be entirely in memory
            int cloudletMinMemoryToExecute = currentRecord.get(2);
            // I am assuming min storage meaning the output size of the cloudlet
            int cloudletMinStorageToExecute = currentRecord.get(3);
            int deadline = currentRecord.get(4);

            List<Cloudlet> currentList = cloudletMap.getOrDefault(cloudletSubmissionTime, new ArrayList<>());
            Cloudlet newCloudlet = new Cloudlet(i, cloudletMIs, 1, cloudletMinMemoryToExecute,
                    cloudletMinStorageToExecute, utilizationModel, utilizationModel, utilizationModel);
            newCloudlet.setUserId(brokerId);
            newCloudlet.setSubmissionTime(Integer.valueOf(cloudletSubmissionTime).doubleValue());

            if (currentList.isEmpty()) {
                cloudletMap.put(cloudletSubmissionTime, currentList);
            }
            currentList.add(newCloudlet);
        }

        return cloudletMap;
    }

    private static List<Cloudlet> createCloudletListsFromRecords(int brokerId, List<List<Integer>> records) {

        List<Cloudlet> cloudlets = new ArrayList<>();

        UtilizationModel utilizationModel = new UtilizationModelFull();
        for (int i = 0; i < records.size(); i++) {
            List<Integer> currentRecord = records.get(i);

            // in whatever CloudSim uses as time unit (which I think is ms)
            int cloudletSubmissionTime = currentRecord.get(0);
            int cloudletMIs = currentRecord.get(1);
            // I am assuming min memory meaning the file size of the cloudlet has to be entirely in memory
            int cloudletMinMemoryToExecute = currentRecord.get(2);
            // I am assuming min storage meaning the output size of the cloudlet
            int cloudletMinStorageToExecute = currentRecord.get(3);
            int deadline = currentRecord.get(4);

            Cloudlet newCloudlet = new Cloudlet(i, cloudletMIs, 1, cloudletMinMemoryToExecute,
                    cloudletMinStorageToExecute, utilizationModel, utilizationModel, utilizationModel);
            newCloudlet.setUserId(brokerId);
            newCloudlet.setDelay(Integer.valueOf(cloudletSubmissionTime).doubleValue());
            newCloudlet.setDeadline(Integer.valueOf(deadline).doubleValue());

            cloudlets.add(newCloudlet);
        }

        return cloudlets;
    }

    private static List<List<Integer>> readRecordsFromCsv() throws IOException, URISyntaxException {
        List<List<Integer>> records = new ArrayList<>();

        InputStream csvFile = Assignment.class.getClassLoader().getResourceAsStream("assignment.csv");

        if (csvFile == null) {
            Log.printLine("assignment.csv not located in resource folder, please check");
            System.exit(100);
        }

        try (InputStreamReader inputStreamReader = new InputStreamReader(csvFile);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        ) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                List<Integer> record = Stream.of(line.split(CSV_SEPARATOR)).map(Integer::parseInt).collect(Collectors.toList());
                records.add(record);
            }
        }

        return records;
    }

    /**
     * Prints the Cloudlet objects
     * @param list  list of Cloudlets
     */
    private static void printCloudletList(List<Cloudlet> list) {
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        System.out.format("%-16s%-32s%-16s%-8s%-12s%-12s%-12s%-12s%-12s%n", "Cloudlet ID", "Status", "Data center ID",
                "VM ID", "Time", "Start Time", "Finish Time", "Deadline", "How Late");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (Cloudlet cloudlet : list) {
            System.out.format("%-16s%-32s%-16s%-8s%-12s%-12s%-12s%-12s%-12s%n", cloudlet.getCloudletId(),
                    cloudlet.getCloudletStatusString(), cloudlet.getResourceId(), cloudlet.getVmId(),
                    dft.format(cloudlet.getActualCPUTime()), dft.format(cloudlet.getExecStartTime()),
                    dft.format(cloudlet.getFinishTime()), dft.format(cloudlet.getDeadline()),
                    dft.format(cloudlet.calculateLateness()));
        }
    }
}
