import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class Tasks extends Thread {

	public static void task1() {
		TreeMap<String, Integer> simpleMap = new TreeMap<String, Integer>();
		log("•	Determine the number of flights from each airport; include a list of any airports not used.\n", fw);
		log("---------------------------------\n", fw);
		try {
			// Read input
			BufferedReader br = new BufferedReader(new FileReader(Passenger));
			String line;
			ArrayList<String> unMatchedAirports = new ArrayList<String>();
			while ((line = br.readLine()) != null) {
				String[] split = line.split(",");
				if (regexThis(split, 1)) {
					if (simpleMap.containsKey(split[2])) {
						simpleMap.put(split[2], simpleMap.get(split[2]) + 1); // calculate number of flights from each airport
					} else {
						simpleMap.put(split[2], 1);
					}
				} else {
					unMatchedAirports.add(split[2]);
				}
			}
			br.close();

			// Write output to file
			for (Entry<String, Integer> entry : simpleMap.entrySet()) {
				log(entry.getKey() + " : " + entry.getValue(), fw);
				log(System.getProperty("line.separator"), fw);
			}

			System.out.println("");
			log("Unused airports (" + unMatchedAirports.size() + "): " + System.getProperty("line.separator"), fw);
			for (String airport : unMatchedAirports) {
				if (!simpleMap.containsKey(airport)) {
					log(airport + ", ", fw);
					
				}
			}

			log(System.getProperty("line.separator"), fw);

			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static TreeMap<String, List<Flight_mapper>> flights = new TreeMap<String, List<Flight_mapper>>();

	public static void task2() {
		// Map
		log("•	Create a list of flights based on the Flight id, this output should include the passenger Id, relevant IATA/FAA codes, the departure time, the arrival time (times to be converted to HH:MM:SS format), and the flight times.\n", fw);
		log("---------------------------------\n", fw);
		try {
			BufferedReader br = new BufferedReader(new FileReader(Passenger));
			String line;
			while ((line = br.readLine()) != null) {
				String[] split = line.split(",");
				if (regexThis(split, 2)) {
					try {
						if (flights.get(split[1]) == null) {
							flights.put(split[1], new ArrayList<Flight_mapper>());
						}
						flights.get(split[1])
								.add(new Flight_mapper(split[0], split[1], split[2], split[3], split[4], split[5]));
					} catch (Exception e) {
						System.out.println("Failed to add <" + split[2].toString() + "> to map.");
					}
				}
			}
			br.close();
			log(ls, fw);
			// Reduce
			for (Map.Entry<String, List<Flight_mapper>> entry : flights.entrySet()) {
				log(entry.getKey(), fw);
				log(ls, fw);
				for (Flight_mapper detail : entry.getValue()) {
					log(detail.toString(), fw);
					log(ls, fw);
				}
				log(ls, fw);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void task3() {
		log("•	Calculate the number of passengers on each flight.\n", fw);
		log("Passenger Count"+ls, fw);
		log("---------------------------------"+ls, fw);
		// MapReduce
		try {
			TreeMap<String, Integer> passengerCount = new TreeMap<String, Integer>();
			BufferedReader br = new BufferedReader(new FileReader(Passenger));
			String line;
			while ((line = br.readLine()) != null) {
				String[] split = line.split(",");
				if (regexThis(split, 3)) {
					if (passengerCount.get(split[1]) == null) {
						passengerCount.put(split[1], 1);
					} else {
						passengerCount.put(split[1], passengerCount.get(split[1]) + 1);
					}
				}
			}
			br.close();
			for (Map.Entry<String, Integer> entry : passengerCount.entrySet()) {
				log(entry.getKey() + ": " + entry.getValue(), fw);
				log(ls, fw);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void task4() {
		// Reduce each passenger to be combined (in case some passengers have more than one flight)
		log("•	Calculate the line-of-sight (nautical) miles for each flight and the total travelled by each passenger and thus output the passenger having earned the highest air miles.\n", fw);
		log(ls, fw);
		log("Distance Travelled Per Flight"+ls, fw);
		log("---------------------------------", fw);
		// Map
		try {
			TreeMap<String, Airport_mapper> airports = new TreeMap<String, Airport_mapper>();
			BufferedReader br = new BufferedReader(new FileReader(Airport));
			String line;

			while ((line = br.readLine()) != null) {
				String[] split = line.split(",");
				if (regexThis(split, 4)) {
					try {
						if (airports.get(split[1]) == null) {
							airports.put(split[1], new Airport_mapper(split[1], split[2], split[3]));
						}
					} catch (Exception e) {
						System.out.println("Failed to add <" + split[0] + "> to map.");
					}
				}
			}
			br.close();
			log(ls, fw);
			// Reduce
			TreeMap<String, Double> distances = new TreeMap<String, Double>();
			
			for (Map.Entry<String, List<Flight_mapper>> entry : flights.entrySet()) {
				for (Flight_mapper aFlight : entry.getValue()) {
					try {
						double x1 = ((Airport_mapper) airports.get(aFlight.getFrom())).getLat();
						double y1 = ((Airport_mapper) airports.get(aFlight.getFrom())).getLon();
						double x2 = ((Airport_mapper) airports.get(aFlight.getTo())).getLat();
						double y2 = ((Airport_mapper) airports.get(aFlight.getTo())).getLon();

						double distance = Math.sqrt((x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1));
						 // Save each distance in a map
						distances.put(entry.getKey(), distance);
						
					} catch (Exception e) {
						log("Couldn't calculate a distance for flight " + aFlight.getFrom() + " to " + aFlight.getTo() + ".", fw);
						log(ls, fw);
					}
				}
			}
			// Reduce Flight ID distances to one record of distance only.
			Map<String, Double> reducer = new TreeMap<String, Double>();
			for (Map.Entry<String, Double> entry : distances.entrySet()) {
				reducer.put(entry.getKey(), entry.getValue());
			}
			for (Map.Entry<String, Double> entry : reducer.entrySet()) {
				log(entry.getKey() + " -> " + Math.round(entry.getValue()) + " miles.", fw);
				log(ls, fw);
			
			}
			
			log(ls, fw);
			log("Distance Travelled Per Passenger"+ls, fw);
			log("---------------------------------"+ls, fw);
			// Stored Key-Value pairs of passengers and distances travelled.
			TreeMap<String, Double> passengerDistance = new TreeMap<String, Double>();
			for (Map.Entry<String, List<Flight_mapper>> aFlight : flights.entrySet()) {
					for(Flight_mapper passenger : aFlight.getValue()){
						if (passengerDistance.get(passenger.getPassengerID()) == null) {
							passengerDistance.put(passenger.getPassengerID(), distances.get(passenger.getFlightID()));
						} else {
							double distance = passengerDistance.get(passenger.getPassengerID());
							passengerDistance.put(passenger.getPassengerID(), distance + distances.get(passenger.getFlightID()));
						}
					}
			}
			
			for (Map.Entry<String, Double> entry : passengerDistance.entrySet()) {
				log(entry.getKey() + " -> " + Math.round(entry.getValue()) + " miles.", fw);
				log(ls, fw);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//Run function for Thread	
	public void run() {
		try {
			Passenger = "AComp_Passenger_data.csv";
			Airport = "Top30_airports_LatLong.csv";
			outputFile = "output.txt";
			fw = new FileWriter(outputFile);
		} catch (Exception e) {
			System.out.println("Input or output file missing.");
			System.exit(0);
		}

		try {
			// Read input from file
			task1();
			task2();
			task3();
			task4();
			
			fw.flush();
			fw.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//-------Main Function-----
	static String ls = System.getProperty("line.separator");
	static String Passenger; // = "E:\\Notes\\Cloud Computing\\Data for Assignment\\AComp_Passenger_data.csv";
	static String Airport; // = "E:\\Notes\\Cloud Computing\\Data for Assignment\\Top30_airports_LatLong.csv";
	static String outputFile; // = "E:\\Notes\\Cloud Computing\\Data for Assignment\\output.txt";
	static FileWriter fw;
	
	public static void main(String[] args) throws FileNotFoundException {
		//Starting the thread
		Tasks obj = new Tasks();
		obj.start();
		
	}
	
	//File Cleaning
	private static boolean regexThis(String[] split, int task) {
		switch (task) {
		case 1:
			if (split.length < 3)
				return false;
			return split[2].matches("[A-Z]{3}");
		case 2:
			if (split.length < 6)
				return false;
			return split[0].matches("[A-Z]{3}[0-9]{4}[A-Z]{2}[0-9]{1}") & split[1].matches("[A-Z]{3}[0-9]{4}[A-Z]{1}")
					& split[2].matches("[A-Z]{3}") & split[3].matches("[A-Z]{3}") & split[4].matches("\\d{10}")
					& split[5].matches("\\d{1,4}");
		case 3:
			if (split.length < 2)
				return false;
			return split[1].matches("[A-Z]{3}[0-9]{4}[A-Z]{1}");
		case 4:
			if (split.length < 4)
				return false;
			
			return split[1].matches("[A-Z]{3}") & split[2].matches("[+-]?\\d*\\.?\\d*")
					& split[3].matches("[+-]?\\d*\\.?\\d*");
		default:
			return false;
		}
	}
	
	
	public static void log(String toLog, FileWriter fw){
		try {
			fw.write(toLog);
			System.out.print(toLog);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
