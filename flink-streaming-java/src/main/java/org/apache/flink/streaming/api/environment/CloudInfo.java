package org.apache.flink.streaming.api.environment;

public class CloudInfo {
	private String cloudId;
	public CloudInfo(String cloudId){
		this.cloudId = cloudId;
	}

	public String getCloudId() {
		return cloudId;
	}

	public void setCloudId(String cloudId) {
		this.cloudId = cloudId;
	}
}
