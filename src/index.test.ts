import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { DynamoDB } from "aws-sdk";
import { Locker, Status } from "./index";
import os from "os";
const fixedDate = new Date("2000-01-02T03:04:05Z");
jest.useFakeTimers("modern").setSystemTime(fixedDate);

interface APIResult {
  data: string;
}

describe("Example", () => {
  it("provides a demo", async () => {
    // Prepare for the demo.
    const db = await createLocalTable();
    try {
      // Demo.
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
    } finally {
      // Tidy up after the demo.
      await db.delete();
    }
  });
});

describe("Locker", () => {
  describe("begin and end", () => {
    it("can begin a new lock", async () => {
      const db = await createLocalTable();
      try {
        const locker = new Locker(db.client, db.name, 60);

        const lock = await locker.begin("abc", "test1", 1000);

        expect(lock.existing).toEqual(null);
        expect(lock.endWithSuccess).not.toBeNull();
        expect(lock.endWithError).not.toBeNull();
      } finally {
        await db.delete();
      }
    });
    it("does not allow two locks with the same key", async () => {
      const db = await createLocalTable();
      try {
        const locker = new Locker(db.client, db.name, 60);
        const responseA = await locker.begin("token", "test1", 1000);
        expect(responseA.existing).toEqual(null);
        const responseB = await locker.begin("token", "test2", 1000);
        expect(responseB.existing).not.toEqual(null);
        expect(responseB.existing.token).toEqual("token");
        expect(responseB.existing.lockedBy).toEqual("test1");
        expect(responseB.existing.created).toEqual(fixedDate);
        expect(responseB.existing.expectedCompletion).toEqual(
          new Date(fixedDate.getTime() + 1000)
        );
        expect(responseB.existing.status).toEqual(Status.InProgress);
        expect(responseB.existing.actualCompletion).toBeNull();
      } finally {
        await db.delete();
      }
    });
    it("doesn't set a TTL to automatically delete the record by default", async () => {
      const db = await createLocalTable();
      try {
        const locker = new Locker<APIResult>(db.client, db.name);
        await locker.begin("token", "test1", 1000);

        const locks = await locker.query(Status.InProgress);

        expect(locks.length).toEqual(1);
        expect(locks.filter((r) => r.token === "token")[0].ttl).toBeNull();
      } finally {
        await db.delete();
      }
    });
    it("can set a TTL to automatically delete the record", async () => {
      const db = await createLocalTable();
      try {
        const ttlMinutes = 60;
        const locker = new Locker<APIResult>(db.client, db.name, ttlMinutes);
        await locker.begin("token", "test1", 1000);
        const locks = await locker.query(Status.InProgress);

        expect(locks.length).toEqual(1);
        expect(locks.filter((r) => r.token === "token")[0].ttl).toEqual(
          fixedDate.getTime() + ttlMinutes * 60000
        );
      } finally {
        await db.delete();
      }
    });
    it("returns the completed value if a lock has been successfully ended", async () => {
      const db = await createLocalTable();
      try {
        const locker = new Locker<APIResult>(db.client, db.name, 60);

        const responseA = await locker.begin("token", "test1", 1000);
        expect(responseA.existing).toEqual(null);
        const actualResult = { data: "test" } as APIResult;
        await responseA.endWithSuccess(actualResult);

        const responseB = await locker.begin("token", "test2", 1000);
        expect(responseB.existing).not.toEqual(null);
        expect(responseB.existing.token).toEqual("token");
        expect(responseB.existing.lockedBy).toEqual("test1");
        expect(responseB.existing.created).toEqual(fixedDate);
        expect(responseB.existing.expectedCompletion).toEqual(
          new Date(fixedDate.getTime() + 1000)
        );
        expect(responseB.existing.result).toEqual(actualResult);
        expect(responseB.existing.status).toEqual(Status.Complete);
        expect(responseB.existing.actualCompletion).toEqual(fixedDate);
      } finally {
        await db.delete();
      }
    });
    it("can handle a null success value", async () => {
      const db = await createLocalTable();
      try {
        const locker = new Locker<APIResult>(db.client, db.name, 60);

        const responseA = await locker.begin("token", "test1", 1000);
        expect(responseA.existing).toEqual(null);
        await responseA.endWithSuccess(null);

        const responseB = await locker.begin("token", "test2", 1000);
        expect(responseB.existing).not.toEqual(null);
        expect(responseB.existing.token).toEqual("token");
        expect(responseB.existing.lockedBy).toEqual("test1");
        expect(responseB.existing.created).toEqual(fixedDate);
        expect(responseB.existing.expectedCompletion).toEqual(
          new Date(fixedDate.getTime() + 1000)
        );
        expect(responseB.existing.result).toEqual(null);
        expect(responseB.existing.status).toEqual(Status.Complete);
        expect(responseB.existing.actualCompletion).toEqual(fixedDate);
      } finally {
        await db.delete();
      }
    });
    it("returns the completed value, along with error information, when an error is tracked", async () => {
      const db = await createLocalTable();
      try {
        const locker = new Locker<APIResult>(db.client, db.name, 60);

        const responseA = await locker.begin("token", "test1", 1000);
        expect(responseA.existing).toEqual(null);
        // Make sure to use JSON serializable errors, not built-in errors.
        await responseA.endWithError("error message, or JSON payload");

        const responseB = await locker.begin("token", "test2", 1000);
        expect(responseB.existing).not.toEqual(null);
        expect(responseB.existing.token).toEqual("token");
        expect(responseB.existing.lockedBy).toEqual("test1");
        expect(responseB.existing.created).toEqual(fixedDate);
        expect(responseB.existing.expectedCompletion).toEqual(
          new Date(fixedDate.getTime() + 1000)
        );
        expect(responseB.existing.result).toEqual(null);
        expect(responseB.existing.error).toEqual(
          "error message, or JSON payload"
        );
        expect(responseB.existing.status).toEqual(Status.Error);
        expect(responseB.existing.actualCompletion).toEqual(fixedDate);
      } finally {
        await db.delete();
      }
    });
  });
  describe("query", () => {
    it("can return a list of inProgress locks", async () => {
      const db = await createLocalTable();
      try {
        const locker = new Locker<APIResult>(db.client, db.name, 60);
        await createTestJobs(locker);

        const locks = await locker.query(Status.InProgress);

        expect(locks.length).toEqual(2);
        expect(locks.filter((r) => r.token === "inProgress1")).toHaveLength(1);
        expect(locks.filter((r) => r.token === "inProgress2")).toHaveLength(1);
      } finally {
        await db.delete();
      }
    });
    it("can return a list of error locks", async () => {
      const db = await createLocalTable();
      try {
        const locker = new Locker<APIResult>(db.client, db.name, 60);
        await createTestJobs(locker);

        const locks = await locker.query(Status.Error);

        expect(locks.length).toEqual(2);
        expect(locks.filter((r) => r.token === "error1")).toHaveLength(1);
        expect(locks.filter((r) => r.token === "error2")).toHaveLength(1);
      } finally {
        await db.delete();
      }
    });
    it("does not provide a list of completed locks, because that could exceed the 10GB partition limit over time", async () => {
      const db = await createLocalTable();
      try {
        const locker = new Locker<APIResult>(db.client, db.name, 60);
        await createTestJobs(locker);

        const locks = await locker.query(Status.Complete);

        expect(locks.length).toEqual(0);
      } finally {
        await db.delete();
      }
    });
  });
});

const createTestJobs = async (locker: Locker<APIResult>) => {
  // Create 2 inProgress jobs.
  const responseA = await locker.begin("inProgress1", "test1", 0);
  expect(responseA.existing).toEqual(null);
  const responseB = await locker.begin("inProgress2", "test1", 1000);
  expect(responseB.existing).toEqual(null);
  // Create 2 successfully completed jobs.
  const responseC = await locker.begin("completed1", "test1", 1000);
  expect(responseC.existing).toEqual(null);
  await responseC.endWithSuccess(null);
  const responseD = await locker.begin("completed2", "test1", 1000);
  expect(responseD.existing).toEqual(null);
  await responseD.endWithSuccess(null);
  // Create 2 error jobs.
  const responseE = await locker.begin("error1", "test1", 1000);
  expect(responseE.existing).toEqual(null);
  await responseE.endWithError("error code 1");
  const responseF = await locker.begin("error2", "test1", 1000);
  expect(responseF.existing).toEqual(null);
  await responseF.endWithError("error code 2");
};

interface DB {
  name: string;
  client: DocumentClient;
  delete: () => Promise<any>;
}

const randomTableName = () => `locktest_${new Date().getTime()}`;

const createLocalTable = async (): Promise<DB> => {
  const ddb = new DynamoDB({
    credentials: {
      accessKeyId: "none",
      secretAccessKey: "none",
    },
    region: "eu-west-1",
    endpoint: "http://localhost:8000",
  });
  const tableName = randomTableName();
  await ddb
    .createTable({
      KeySchema: [
        {
          KeyType: "HASH",
          AttributeName: "token",
        },
      ],
      TableName: tableName,
      AttributeDefinitions: [
        {
          AttributeName: "token",
          AttributeType: "S",
        },
        {
          AttributeName: "status",
          AttributeType: "S",
        },
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: "byStatus",
          KeySchema: [
            {
              KeyType: "HASH",
              AttributeName: "status",
            },
            {
              KeyType: "RANGE",
              AttributeName: "token",
            },
          ],
          Projection: {
            ProjectionType: "ALL",
          },
        },
      ],
      BillingMode: "PAY_PER_REQUEST",
    })
    .promise();

  await ddb.waitFor("tableExists", { TableName: tableName }).promise();

  return {
    name: tableName,
    client: new DocumentClient({
      credentials: {
        accessKeyId: "none",
        secretAccessKey: "none",
      },
      region: "eu-west-1",
      endpoint: "http://localhost:8000",
    }),
    delete: async () =>
      await ddb.deleteTable({ TableName: tableName }).promise(),
  };
};
