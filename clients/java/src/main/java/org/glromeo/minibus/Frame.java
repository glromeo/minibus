package org.glromeo.minibus;

import com.google.gson.JsonElement;

import java.util.UUID;

enum Kind {
  QUEUE,
  TOPIC
}

public class Frame {
  public UUID id;
  public String type; // pub | sub | msg | ack | nack | flow | ping | pong | once
  public Kind kind;           // use enum if you want strictness
  public String source; // client UUID
  public String target; // queue name
  public JsonElement data; // equivalent to json.RawMessage

  public Frame(UUID id, String type, Kind kind, String source, String target, JsonElement data) {
    this.id = id;
    this.type = type;
    this.kind = kind;
    this.source = source;
    this.target = target;
    this.data = data;
  }

  public Frame(String type, Kind kind, String source, String target, JsonElement data) {
    Frame(UUID.randomUUID(), type, kind, source, target, data);
  }
}