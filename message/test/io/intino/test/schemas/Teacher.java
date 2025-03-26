package io.intino.test.schemas;

import java.time.Instant;

public class Teacher extends Person {
	public String university;

	public Teacher() {
	}

	public Teacher(String name, Gender gender, double money, Instant birthDate, Country country, String university) {
		super(name, gender, money, birthDate, country);
		this.university = university;
	}

}
