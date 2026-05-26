# helsemelding-outbound-processing-service

Processes outbound messages from Kafka by validating records, converting message payloads from JSON to XML, and routing them to either an outbound topic or an error topic. Messages sent to the outbound topic are picked up by `helsemelding-outbound-message-service`, which forwards them to the NHN Messages API for further processing.

## Processing

The service:

- Validates Kafka records
- Converts message payloads from JSON to XML
- Forwards valid messages to the outbound topic
- Publishes structured error events for invalid messages

## Validation

The service validates:

- Kafka record key must be a valid UUID
- Kafka record value must be a valid JSON object
- Kafka record must contain a `sourceSystem` header

## Flow

```text
Input Topic
     |
     v
Validation
     |
     v
JSON → XML
     |
+----+----+
|         |
v         v
Valid   Invalid
|         |
v         v
Outbound Error
Topic    Topic
```

## Error handling

Invalid messages are not forwarded.

Instead, one structured error event is published per invalid message, containing one or more validation errors:

```json
{
  "processedAt": "2026-05-21T12:15:42.184Z",
  "errors": [
    {
      "category": "VALIDATION",
      "code": "INVALID_KAFKA_KEY",
      "message": "Kafka record key is not a valid UUID"
    },
    {
      "category": "VALIDATION",
      "code": "MISSING_SOURCE_SYSTEM_HEADER",
      "message": "Kafka record is missing sourceSystem header"
    }
  ],
  "originalMessage": {
    "topic": "input-topic",
    "partition": 1,
    "offset": 12345,
    "createdAt": "2026-05-21T12:15:41.901Z",
    "key": "not-a-uuid"
  }
}
```

Possible error codes:

* `INVALID_KAFKA_KEY`
* `INVALID_KAFKA_VALUE`
* `MISSING_SOURCE_SYSTEM_HEADER`