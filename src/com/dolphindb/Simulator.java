package com.dolphindb;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.BasicDateVector;
import com.xxdb.data.BasicDoubleVector;
import com.xxdb.data.BasicIntVector;
import com.xxdb.data.BasicSecondVector;
import com.xxdb.data.BasicString;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.Entity;
import com.xxdb.data.Utils;

public class Simulator {
	private String path;
	private int rate;
	private DBConnection conn;
	
	private List<String> vsymbol;
	private List<Integer> vdate;
	private List<Integer> vtime;
	private List<Double> vbid;
	private List<Double> vofr;
	private List<Integer> vbidsiz;
	private List<Integer> vofrsiz;
	private List<Integer> vmode;
	private List<String> vex;
	private List<String> vmmid;

	private static final int BATCH_SIZE = 1000;
	private static final String TABLE_1_NAME = "trades";
	//private static final String TABLE_2_NAME = "trades2";
	
	public Simulator(String path, DBConnection conn, int rate) {
		this.path = path;
		this.conn = conn;
		this.rate = rate;
		resetVectors();
	}
	
	public void simulate() {
		long lastTime = -1;
		SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ss");
		DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy.MM.dd");
		long beginAt, endAt, t0, t1, startTime, endTime;
		try {
			beginAt = timeFormatter.parse("09:30:00").getTime();
			endAt = timeFormatter.parse("21:00:00").getTime();
		} catch (ParseException e1) {
			e1.printStackTrace();
			return;
		}

		Iterable<CSVRecord> records;
		try {
			Reader in = new FileReader(path);
			records = CSVFormat.EXCEL
					.withHeader("SYMBOL", "DATE", "TIME", "BID", "OFR", "BIDSIZ", "OFRSIZ", "MODE", "EX", "MMID").parse(in);
		} catch (FileNotFoundException e) {
			System.out.println("Cannot find file: " + path);
			return;
		} catch (IOException e) {
			System.out.println("IOException");
			return;
		}
		
		System.out.println("Start simulation");
		int count = 0;
		int batchCount = 0;
		t0 = startTime = System.currentTimeMillis();

		for (CSVRecord record : records) {
			if (count == 0) {    // header line
				count++;
				continue;
			}
			String time = record.get("TIME");
			long currentTime;    // time of current record
			try {
				currentTime = timeFormatter.parse(time).getTime();
			} catch (ParseException e) {
				System.out.println("Cannot parse time: " + time);
				return;
			}
			if (beginAt != 0 && currentTime < beginAt)
				continue;

			if (count == 1)    // first line
				lastTime = currentTime;
			
			if (endAt != 0 && currentTime >= endAt) {
				runInsert();
				break;
			}

//			if (currentTime > lastTime) {
//				runInsert();
//				resetVectors();
//				batchCount = 0;
//				
//				if (endAt != 0 && currentTime > endAt)
//					break;
//
//				endTime = System.currentTimeMillis();
//				long elapsedTime = endTime - startTime;
//				if (elapsedTime * rate < currentTime - lastTime) {
//					try {
//						TimeUnit.MILLISECONDS.sleep((currentTime - lastTime) / rate - elapsedTime);
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//				}
//				startTime = System.currentTimeMillis();
//			}
//			else {
//				if (batchCount >= BATCH_SIZE) {
//					runInsert();
//					resetVectors();
//					batchCount = 0;
//				}
//			}
			if (batchCount >= BATCH_SIZE) {
				runInsert();
				//uploadAndInsert();
				resetVectors();
				batchCount = 0;
			}

			batchCount++;
			String symbol = record.get("SYMBOL");
			String date = record.get("DATE");
			String bid = record.get("BID");
			String ofr = record.get("OFR");
			String bidsiz = record.get("BIDSIZ");
			String ofrsiz = record.get("OFRSIZ");
			String mode = record.get("MODE");
			String ex = record.get("EX");
			String mmid = record.get("MMID");
			
			vsymbol.add(symbol);
			vdate.add(Utils.countDays(LocalDate.parse(date, dateFormatter)));
			vtime.add(Utils.countSeconds(LocalTime.parse(time)));
			vbid.add(Double.parseDouble(bid));
			vofr.add(Double.parseDouble(ofr));
			vbidsiz.add(Integer.parseInt(bidsiz));
			vofrsiz.add(Integer.parseInt(ofrsiz));
			vmode.add(Integer.parseInt(mode));
			vex.add(ex);
			vmmid.add(mmid);

			lastTime = currentTime;
			count++;
			
			if (count % 100000 == 0) {
				t1 = System.currentTimeMillis();
				System.out.println(count + " messages has been sent. Average time: " + 100000000 / (t1 - t0));
				t0 = t1;
			}
		}
		System.out.println((count - 1) + " messages has been sent.");
		System.out.println("End simulation");
	}
	
	private void resetVectors() {
		vsymbol = new ArrayList<>();
		vdate = new ArrayList<>();
		vtime = new ArrayList<>();
		vbid = new ArrayList<>();
		vofr = new ArrayList<>();
		vbidsiz = new ArrayList<>();
		vofrsiz = new ArrayList<>();
		vmode = new ArrayList<>();
		vex = new ArrayList<>();
		vmmid = new ArrayList<>();
	}
	
	private void uploadAndInsert() {
		Map<String, Entity> result = new HashMap<>();
		result.put("tsymbol", new BasicStringVector(vsymbol));
		result.put("tdate", new BasicDateVector(vdate));
		result.put("ttime", new BasicSecondVector(vtime));
		result.put("tbid", new BasicDoubleVector(vbid));
		result.put("tofr", new BasicDoubleVector(vofr));
		result.put("tbidsiz", new BasicIntVector(vbidsiz));
		result.put("tofrsiz", new BasicIntVector(vofrsiz));
		result.put("tmode", new BasicIntVector(vmode));
		result.put("tex", new BasicStringVector(vex));
		result.put("tmmid", new BasicStringVector(vmmid));
		try {
			conn.upload(result);
			conn.run("insert into " + TABLE_1_NAME + " values (tsymbol, tdate, ttime, tbid, tofr, tbidsiz, tofrsiz, tmode, tex, tmmid)");
		} catch (Exception e) {
			System.out.println("Insert error");
			System.out.println(result.toString());
			e.printStackTrace();
		}
	}
	
	private void runInsert() {
		BasicAnyVector result = new BasicAnyVector(10);
		result.setEntity(0, new BasicStringVector(vsymbol));
		result.setEntity(1, new BasicDateVector(vdate));
		result.setEntity(2, new BasicSecondVector(vtime));
		result.setEntity(3, new BasicDoubleVector(vbid));
		result.setEntity(4, new BasicDoubleVector(vofr));
		result.setEntity(5, new BasicIntVector(vbidsiz));
		result.setEntity(6, new BasicIntVector(vofrsiz));
		result.setEntity(7, new BasicIntVector(vmode));
		result.setEntity(8, new BasicStringVector(vex));
		result.setEntity(9, new BasicStringVector(vmmid));
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
		/*
			n=10000000
			t1=table(n:0, `symbol`date`time`bid`ofr`bidsiz`ofrsiz`mode`ex`mmid, [SYMBOL,DATE,SECOND,DOUBLE,DOUBLE,INT,INT,INT,SYMBOL,SYMBOL])
			share t1 as trades1
			setStream(trades1,true)
			t=NULL
			
			n=10000000
			t1=table(n:0, `symbol`date`time`bid`ofr`bidsiz`ofrsiz`mode`ex`mmid, [STRING,DATE,SECOND,DOUBLE,DOUBLE,INT,INT,INT,STRING,STRING])
			share t1 as trades1
			setStream(trades1,true)
			t=NULL
			h=xdb("192.168.1.25",8801)
			subscribeTable(h,"trades1",,trades1)
		 */
		if (args.length < 3) {
			System.out.println("Usage: Simulator hostname port path/to/csv [rate]");
			return;
		}
		DBConnection conn = new DBConnection();
		String hostname = args[0];
		int port = Integer.parseInt(args[1]);
		String path = args[2];
		int rate = args.length == 3 ? 1 : Integer.parseInt(args[3]);
		try {
			conn.connect(hostname, port);
			new Simulator(path, conn, rate).simulate();
		} catch (NumberFormatException e) {
			System.out.println("Cannot parse port: " + args[1]);
		} catch (IOException e) {
			System.out.println("Connection error");
			e.printStackTrace();
		}
	}
}
