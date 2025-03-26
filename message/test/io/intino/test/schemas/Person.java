package io.intino.test.schemas;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class Person {
	public String name;
	public Gender gender;
	public double money;
	public Instant birthDate;
	public Country country;
	public List<Phone> phones;

	public Person() {
	}

	public Person(String name, Gender gender, double money, Instant birthDate, Country country) {
		this.name = name;
		this.gender = gender;
		this.money = money;
		this.birthDate = birthDate;
		this.country = country;
	}

	public void add(Phone phone) {
		if (phones == null) phones = new ArrayList<>();
		phones.add(phone);

	}

	public enum Gender {
		Male, Female
	}

}
