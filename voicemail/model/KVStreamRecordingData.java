package voicemail.model;
import org.json.JSONObject;

public class KVStreamRecordingData {
	private String fragmentStartNumber;
    private String fragmentStopNumber;
    private String location;
    private String startTimestamp;
    private String stopTimestamp;

    public KVStreamRecordingData(JSONObject jsonObject) {
        this.fragmentStartNumber = jsonObject.getString("FragmentStartNumber");
        this.fragmentStopNumber = jsonObject.getString("FragmentStopNumber");
        this.location = jsonObject.getString("Location");
        this.startTimestamp = jsonObject.getString("StartTimestamp");
        this.stopTimestamp = jsonObject.getString("StopTimestamp");
    }

    public String getFragmentStartNumber() {
        return fragmentStartNumber;
    }

    public String getFragmentStopNumber() {
        return fragmentStopNumber;
    }

    public String getLocation() {
        return location;
    }

    public String getStartTimestamp() {
        return startTimestamp;
    }

    public String getStopTimestamp() {
        return stopTimestamp;
    }
}

}
