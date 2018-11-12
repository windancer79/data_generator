package com.dolphindb;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.lang.Thread.sleep;

public class IotSimulator {

    private int rate;
    private DBConnection conn;

    private List<Integer> vhardwareId;
    private List<Long> vtimestamp;
    private List<Double> velectronicValue;

    private static final int BATCH_SIZE = 1000;
    private static final String TABLE_1_NAME = "sensorData";

    public IotSimulator(DBConnection conn, int rate) {
        this.conn = conn;
        this.rate = rate;
        resetVectors();
    }
    public void Simulator() {
        long recordLimit = 1000000;

        System.out.println("Start simulation");
        int count = 0;
        int batchCount = 0;
        long t0 = System.currentTimeMillis();
        for (int i =0;i< recordLimit;i++) {

            if (batchCount >= BATCH_SIZE) {
                runInsert();
                resetVectors();
                batchCount = 0;
            }

            batchCount++;

            vhardwareId.add(1);
            vtimestamp.add(Utils.countMilliseconds(LocalDateTime.now()));
            velectronicValue.add(getRandomDouble());

            count++;

            if (count % 100000 == 0) {
                long t1 = System.currentTimeMillis();
                System.out.println(count + " messages has been sent. Average time: " + 100000000 / (t1 - t0));
                t0 = t1;
            }
            try{
                sleep(rate);
            }catch (InterruptedException ite){
                ite.printStackTrace();
            }

        }
        System.out.println((count - 1) + " messages has been sent.");
        System.out.println("End simulation");
    }

    private double getRandomDouble(){
        Random r = new Random();
        return r.nextDouble();
    }
    private void resetVectors() {
        vhardwareId = new ArrayList<>();
        vtimestamp = new ArrayList<>();
        velectronicValue = new ArrayList<>();
    }
    private void runInsert() {
        BasicAnyVector result = new BasicAnyVector(3);
        result.setEntity(0, new BasicTimestampVector(vtimestamp));
        result.setEntity(1, new BasicIntVector(vhardwareId));
        result.setEntity(2, new BasicDoubleVector(velectronicValue));
        ArrayList<Entity> arguments = new ArrayList<>(2);
        arguments.add(new BasicString(TABLE_1_NAME));
        arguments.add(result);
        try {
            conn.run("tableInsert", arguments);
        } catch (Exception e) {
            System.out.println("Insert error");
            System.out.println(result.toString());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        if (args.length < 2) {
//            System.out.println("Usage: Simulator hostname port [rate]");
//            return;
//        }
        DBConnection conn = new DBConnection();
        //String hostname = args[0];
        String hostname = "115.239.209.223";
        //int port = Integer.parseInt(args[1]);
        int port = 8961;
        //int rate = args.length == 2 ? 1 : Integer.parseInt(args[2]);
        try {
            conn.connect(hostname, port);
            new IotSimulator(conn, 1).Simulator();
        } catch (NumberFormatException e) {
            System.out.println("Cannot parse port: " + args[1]);
        } catch (IOException e) {
            System.out.println("Connection error");
            e.printStackTrace();
        }
    }
}
