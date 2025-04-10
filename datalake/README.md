# Datalake
A Java interface for working with data stored in a datalake. This library provides an implementation based on java.io.File.

## Table of Contents
- [EventStore](#eventstore)
- [EntityStore](#entitystore)

## EventStore
The `EventStore` interface provides methods for accessing event data stored in a datalake.

### Methods
- `tanks()`: Returns a `Stream` of `Tank` objects, each representing a different event tank.
- `tank(String name)`: Returns a specific `Tank` object given its name.

#### Tank
The `Tank` interface represents a collection of events in a datalake.

##### Properties
- `name()`: Returns the name of the tank.
- `scale()`: Returns the scale of the tank.

##### Methods
- `tubs()`: Returns a `Stream` of `Tub` objects, each representing a different event tub.
- `first()`: Returns the first `Tub` in the tank.
- `last()`: Returns the last `Tub` in the tank.
- `on(Timetag tag)`: Returns the `Tub` at a specific `Timetag`.
- `content()`: Returns an `EventStream` object representing all events in the tank.
- `content(Predicate<Timetag> filter)`: Returns an `EventStream` object representing all events in the tank that match the given `filter`.

###### Tub
The `Tub` interface represents a collection of events at a specific `Timetag` in a `Tank`.

- `timetag()`: Returns the `Timetag` of the tub.
- `events()`: Returns an `EventStream` object representing all events in the tub.

## EntityStore
The `EntityStore` interface provides methods for accessing entity data stored in a datalake.

### Methods
- `tanks()`: Returns a `Stream` of `Tank` objects, each representing a different entity tank.
- `tank(String name)`: Returns a specific `Tank` object given its name.

#### Tank
The `Tank` interface represents a collection of entities in a datalake.

##### Properties
- `name()`: Returns the name of the tank.

##### Methods
- `tubs()`: Returns a `Stream` of `Tub` objects, each representing a different entity tub.
- `first()`: Returns the first `Tub` in the tank.
- `last()`: Returns the last `Tub` in the tank.
- `on(Timetag tag)`: Returns the `Tub` at a specific `Timetag`.
- `tubs(int count)`: Returns a `Stream` of the most recent `count` `Tub` objects.
- `tubs(Timetag from, Timetag to)`: Returns a `Stream` of `Tub` objects between the `from` and `to` `Timetag` values.

###### Tub
The `Tub` interface represents a collection of entities at a specific `Timetag` in a `Tank`.

- `timetag()`: Returns the `Timetag` of the tub.
- `scale()`: Returns the scale of the tub.
- `triplets()`: Returns a `Stream`