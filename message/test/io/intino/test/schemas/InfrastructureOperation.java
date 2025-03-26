package io.intino.test.schemas;

public class InfrastructureOperation implements java.io.Serializable {

	private java.time.Instant ts;
	private String operation = "";
	private String user = "";
	private String objectType = "";
	private String objectID = "";
	private java.util.List<String> parameters = new java.util.ArrayList<>();

	public java.time.Instant ts() {
		return this.ts;
	}

	public String operation() {
		return this.operation;
	}

	public String user() {
		return this.user;
	}

	public String objectType() {
		return this.objectType;
	}

	public String objectID() {
		return this.objectID;
	}

	public java.util.List<String> parameters() {
		return this.parameters;
	}

	public InfrastructureOperation ts(java.time.Instant ts) {
		this.ts = ts;
		return this;
	}

	public InfrastructureOperation operation(String operation) {
		this.operation = operation;
		return this;
	}

	public InfrastructureOperation user(String user) {
		this.user = user;
		return this;
	}

	public InfrastructureOperation objectType(String objectType) {
		this.objectType = objectType;
		return this;
	}

	public InfrastructureOperation objectID(String objectID) {
		this.objectID = objectID;
		return this;
	}

	public InfrastructureOperation parameters(java.util.List<String> parameters) {
		this.parameters = parameters;
		return this;
	}
}

