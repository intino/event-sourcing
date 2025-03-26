package io.intino.test.schemas;

public class CredentialLogin implements java.io.Serializable {

	private java.time.Instant ts;
	private String authentication = "";
	private java.util.List<Parameter> parameterList = new java.util.ArrayList<>();

	public java.time.Instant ts() {
		return this.ts;
	}

	public String authentication() {
		return this.authentication;
	}

	public java.util.List<Parameter> parameterList() {
		return this.parameterList;
	}

	public CredentialLogin ts(java.time.Instant ts) {
		this.ts = ts;
		return this;
	}

	public CredentialLogin authentication(String authentication) {
		this.authentication = authentication;
		return this;
	}

	public CredentialLogin parameterList(java.util.List<Parameter> parameterList) {
		this.parameterList = parameterList;
		return this;
	}
}