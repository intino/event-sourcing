package io.intino.test.schemas;


public class Parameter implements java.io.Serializable {

	private String name = "";
	private String value = "";

	public Parameter() {
	}

	public Parameter(String name, String value) {
		this.name = name;
		this.value = value;
	}

	public String name() {
		return this.name;
	}

	public String value() {
		return this.value;
	}

	public Parameter name(String name) {
		this.name = name;
		return this;
	}

	public Parameter value(String value) {
		this.value = value;
		return this;
	}
}