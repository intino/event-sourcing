# Sealer

A Java module for sealing sessions into a datalake.  
The `SessionSealer` interface provides the functionality to consolidate event sessions into the appropriate tanks within a `Datalake`, ensuring events are persisted in an ordered and structured manner.

## Table of Contents
- [SessionSealer](#sessionsealer)
- [TankFilter](#tankfilter)
- [TankNameFilter](#tanknamefilter)
- [Usage](#usage)

## SessionSealer

The `SessionSealer` interface is responsible for incorporating sessions into the datalake. Sealing is the process of persisting the events from a session into the corresponding tanks in a consistent and ordered fashion.

### Methods

- `seal()`: Seals all available tanks by default (equivalent to `seal(TankFilter.acceptAll())`).
- `seal(TankFilter tankFilter)`: Seals only the tanks accepted by the given `TankFilter`.

## TankFilter

A functional interface used to filter which `Tank` objects in the datalake should be affected during sealing.

### Static Methods

- `acceptAll()`: Returns a `TankFilter` that accepts all tanks.

### Methods

- `accepts(Datalake.Store.Tank<?> tank)`: Returns `true` if the tank should be included in the sealing process.
- `test(Datalake.Store.Tank<?> tank)`: Alias for `accepts`.

## TankNameFilter

A functional interface used to filter tanks by name.

### Static Methods

- `acceptAll()`: Returns a `TankNameFilter` that accepts all tank names.

### Methods

- `accepts(String tankName)`: Returns `true` if the tank name should be accepted.
- `test(String tankName)`: Alias for `accepts`.

## Usage

A typical use case involves applying a `SessionSealer` after collecting event data into a session, in order to consolidate it into the datalake:

```java
SessionSealer sealer = ...;

// Seal all tanks
sealer.seal();

// Seal only tanks whose name starts with "sensor"
sealer.seal(tank -> tank.name().startsWith("sensor"));