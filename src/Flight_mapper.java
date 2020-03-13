import java.text.SimpleDateFormat;
import java.util.Date;

public class Flight_mapper {
	String passengerID;
	String flightID;
	String from;
	String to;
	String unixArrival;
	String arrivalTime;
	int flightTime;

	
	public Flight_mapper(String _passID, String _flightID, String _from, String _to, String _unixArrival, String _flightTime) {
		passengerID = _passID;
		flightID = _flightID;
		
		from = _from;
		to = _to;
		
		unixArrival = _unixArrival;
		arrivalTime = unixToHHMMSS(unixArrival);
		
		flightTime = Integer.parseInt(_flightTime);
	}
	
	private String unixToHHMMSS(String unixTime){
		int temp = flightTime * 60000;
		long unixSeconds = Long.parseLong(unixTime);
		Date date = new Date((unixSeconds*1000L) + temp); // secs to millisecs
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss"); // Date Format
//		date = date.getMinutes() + flightTime;
		String formattedDate = sdf.format(date);
		return formattedDate;
	}
	public String getFrom(){
		return from;
	}
	
	public String getPassengerID(){
		return passengerID;
	}
	
	public String getFlightID(){
		return flightID;
	}
	public String getTo(){
		return to;
	}
	String convertArrivalTime(String seconds){
		return new String(seconds);
	}
	
	public String toString(){
		return "Passenger ID: " + passengerID + " To: " + to + " From: " + from + " ArrivalTime: " + arrivalTime + " FlightTime: " + flightTime + " mins";
	}
}
