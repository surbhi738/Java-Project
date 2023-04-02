package voicemail.model;

package com.amazonaws.awsconnect.model;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class ContactTraceRecord {

    private String awsAccountId;
    private String contactId;
    private CustomerEndpoint customerEndpoint;
    private ContactFlowAttributes attributes;
    private List<KVStreamRecordingData> recordings = new ArrayList<>();

    public ContactTraceRecord(JSONObject jsonObject) {
        this.awsAccountId = jsonObject.getString("AWSAccountId");
        this.contactId = jsonObject.getString("ContactId");
        this.customerEndpoint =  new CustomerEndpoint(jsonObject.getJSONObject("CustomerEndpoint"));
        this.attributes = new ContactFlowAttributes(jsonObject.getJSONObject("Attributes"));
        JSONArray recordings = jsonObject.getJSONArray("Recordings");
        for (int i = 0; i < recordings.length(); i++) {
            this.recordings.add(new KVStreamRecordingData(recordings.getJSONObject(i)));
        }
    }

    public boolean hasRecordings() {
        return recordings.size() > 0;
    }

    public List<KVStreamRecordingData> getRecordings() {
        return recordings;
    }

    public String getAwsAccountId() {
        return awsAccountId;
    }

    public ContactFlowAttributes getAttributes() {
        return attributes;
    }

    public String getContactId() {
        return contactId;
    }

    public CustomerEndpoint getCustomerEndpoint() {
        return customerEndpoint;
    }
}