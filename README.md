# helsemelding-outbound-processing-service

Processes outbound dialog messages from Kafka. The service receives JSON messages, validates the Kafka record and message payload, converts valid messages to XML, and publishes either the XML payload or a structured error message.

## Flow

```text
helsemelding.dialog.out
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

Successfully converted XML messages are published with `OutgoingDialogMessage.id` as the Kafka key. Error messages are published without a Kafka key.

## Validation

The service validates:

- Kafka record value exists and is not empty
- Kafka record has a non-empty `sourceSystem` header
- Kafka record value is a valid outgoing dialog message according to the JSON schema

Validation and conversion failures are published to the error topic.

## Topics

Default topic config:

- Input JSON: `helsemelding.dialog.out`
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
      "code": "MISSING_SOURCE_SYSTEM_HEADER",
      "message": "Kafka record header 'sourceSystem' is missing or empty"
    }
  ],
  "originalMessage": {
    "createdAt": "2026-05-21T12:15:41.901Z",
    "payload": "{\"hello\":\"world\"}"
  }
}
```

Conversion failures may originate from message conversion, PDL lookups, or provider registry lookups.

Error codes:

- `INVALID_KAFKA_VALUE`
- `MISSING_SOURCE_SYSTEM_HEADER`
- `INVALID_MESSAGE`
- `CONVERSION_ERROR`
- `MESSAGE_ID_EXTRACTION_ERROR`
- `PDL_ERROR`
- `PROVIDER_REGISTRY_ERROR`
- `SIGNING_ERROR`
