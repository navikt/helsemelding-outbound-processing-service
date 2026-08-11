# helsemelding-outbound-processing-service

Processes outbound dialog messages from Kafka. The service receives JSON messages, validates the Kafka record and message payload, converts valid messages to XML, and publishes either the XML payload or a structured error message.

## Flow

```text
helsemelding.dialog.out.json
    |
    v
MessageReceiver
    |
    v
MessageProcessingService
    |
    +-- invalid record/message --> helsemelding.dialog.out.error
    |
    +-- valid JSON --> message-converter --> helsemelding.dialog.out.xml
```

The XML topic is consumed by `helsemelding-outbound-message-service`, which forwards messages to the NHN Messages API.

## Validation

The service validates:

- Kafka record key exists and is a valid UUID
- Kafka record value exists and is not empty
- Kafka record has a non-empty `sourceSystem` header
- Kafka record value is a valid outgoing dialog message according to the JSON schema

Validation failures are published to the error topic. Conversion failures are also published to the error topic.

## Topics

Default topic config:

- Input JSON: `helsemelding.dialog.out.json`
- Output XML: `helsemelding.dialog.out.xml`
- Error messages: `helsemelding.dialog.out.error`

## Error Message

Example error message:

```json
{
  "processedAt": "2026-05-21T12:15:42.184Z",
  "sourceSystem": "UNKNOWN",
  "errors": [
    {
      "category": "VALIDATION",
      "code": "INVALID_KAFKA_KEY",
      "message": "Kafka record key is not a valid UUID"
    }
  ],
  "originalMessage": {
    "createdAt": "2026-05-21T12:15:41.901Z",
    "key": "not-a-uuid",
    "payload": "{\"hello\":\"world\"}"
  }
}
```

Error codes:

- `INVALID_KAFKA_KEY`
- `INVALID_KAFKA_VALUE`
- `MISSING_SOURCE_SYSTEM_HEADER`
- `INVALID_MESSAGE`
- `CONVERSION_ERROR`
