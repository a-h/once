# Locker

Typescript library for the creation of idempotent APIs, and idempotent processing of messages, using DynamoDB.

## Usage example

```typescript
const apiRequest = { orderId: "12345" };

// Use locker to make the response idempotent.
const locker = new Locker<APIResult>(db.client, db.name);
const expectedDurationMs = 60000;
const lock = await locker.begin(
  "order/create/" + apiRequest.orderId,
  os.hostname(),
  expectedDurationMs
);

// If we didn't create a new lock let's return what
// we've got.
if (lock.existing != null) {
  switch (lock.existing.status) {
    case Status.Error:
      // The error is just a string.
      return JSON.parse(lock.existing.error);
    case Status.InProgress:
      // Something else is doing the work right now.
      throw new Error(
        "Work is being done by another process, please try again later."
      );
    case Status.Complete:
      // It's already been done once, return the cached result.
      return lock.existing.result;
  }
}

// We created a new one, so do the work.
try {
  // Do your work here, then call endWithSuccess.
  const result = { data: "success" } as APIResult;
  await lock.endWithSuccess(result);
  return result;
} catch (e) {
  // Errors are fatal, they'll need human interaction.
  await lock.endWithError(JSON.stringify({ error: "oh no" }));
}
```

## Development

Run `docker-compose up` to bring up the local DynamoDB instance used for integration tests.
