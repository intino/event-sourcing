# Session

A Java module for handling sessions in the context of datalake event ingestion.  
A `Session` provides a structured way to group and import event data, supporting specific formats and input sources.

## Table of Contents
- [Session](#session)
- [Constants](#constants)

## Session

The `Session` interface represents a structured data source containing events to be inserted into the datalake.

### Properties

- `name()`: Returns the name of the session, typically used as an identifier.
- `format()`: Returns the `Format` of the events contained in the session (e.g., JSON, XML, etc.).
- `inputStream()`: Returns an `InputStream` to access the raw content of the session.

### Constants

- `Session.SessionExtension`: The standard file extension for session files (`.session`).

## Format

The `Format` enum (defined in `systems.intino.eventsourcing.event.Event`) specifies the serialization format used in the session.

## Usage

A `Session` instance is typically used to load and process a batch of events from an input stream, such as a file or a network source, while preserving metadata like format and name.
