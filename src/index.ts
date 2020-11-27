import { Converter, DocumentClient } from "aws-sdk/clients/dynamodb";

// LockRecord is the internal DynamoDB structure.
export interface LockRecord {
  token: string;
  lockedBy: string;
  created: Date;
  expectedCompletion: Date;
  status: string;

  // These fields are only present on completed locks.
  actualCompletion: Date | null;
  result: string;
  error: string;

  // This marks when the record should be deleted by DynamoDB.
  ttl: number | null;
}

export interface BeginResult<TResult> {
  existing: Lock<TResult>;
  endWithSuccess: (result: TResult) => Promise<void>;
  endWithError: (error: string) => Promise<void>;
}

// A lock that has been started.
export interface Lock<TResult> {
  token: string;
  lockedBy: string;
  created: Date;
  expectedCompletion: Date;
  status: Status;
  actualCompletion: Date | null;
  result: TResult | null;
  error: string | null;
}

// Status of the lock.
export enum Status {
  InProgress = "inProgress",
  Complete = "complete",
  Error = "error",
}

export class Locker<T> {
  client: DocumentClient;
  table: string;
  ttlMins: number;
  constructor(client: DocumentClient, table: string, ttlMins: number = 0) {
    this.client = client;
    this.table = table;
    this.ttlMins = ttlMins;
  }
  async begin(
    token: string,
    lockedBy: string,
    expectedDurationMs: number
  ): Promise<BeginResult<T>> {
    const created = new Date();
    const params = {
      TransactItems: [
        {
          Put: {
            TableName: this.table,
            Item: {
              token,
              lockedBy,
              created: created.toISOString(),
              expectedCompletion: new Date(
                created.getTime() + expectedDurationMs
              ).toISOString(),
              status: "inProgress",
              ttl:
                this.ttlMins > 0
                  ? created.getTime() + this.ttlMins * 60000
                  : null,
            },
            ConditionExpression: "attribute_not_exists(#token)",
            ExpressionAttributeNames: {
              "#token": "token",
            },
            ReturnValuesOnConditionCheckFailure: "ALL_OLD",
          } as DocumentClient.Put,
        } as DocumentClient.TransactWriteItem,
      ],
    } as DocumentClient.TransactWriteItemsInput;
    const transactWrite = this.client.transactWrite(params);
    const beginResult = {
      existing: null,
      endWithSuccess: async (result: T) =>
        await this.endWithSuccess(token, lockedBy, result),
      endWithError: async (error: string) =>
        await this.endWithError(token, lockedBy, error),
    } as BeginResult<T>;

    // This is the only way to read the CancellationReasons and retrieve
    // the ReturnValuesOnConditionCheckFailure value.
    transactWrite.on("extractError", (res) => {
      const reasons = JSON.parse(res.httpResponse.body.toString())
        .CancellationReasons as any[];
      if (Array.isArray(reasons) && reasons.length > 0) {
        const record = Converter.unmarshall(reasons[0].Item) as LockRecord;
        beginResult.existing = {
          token: record.token,
          lockedBy: record.lockedBy,
          created: new Date(record.created),
          expectedCompletion: new Date(record.expectedCompletion),
          status: record.status ? (record.status as Status) : Status.Complete,
          result: record.result ? (JSON.parse(record.result) as T) : null,
          error: record.error,
          actualCompletion: record.actualCompletion
            ? new Date(record.actualCompletion)
            : null,
        };
      }
    });

    // The promise checks to see whether an error happened.
    return new Promise((resolve, reject) => {
      transactWrite.send((err, _response) => {
        if (err) {
          if (beginResult.existing == null) {
            return reject(err);
          }
        }
        return resolve(beginResult);
      });
    });
  }
  private async endWithSuccess(
    token: string,
    lockedBy: string,
    result: T
  ): Promise<void> {
    const params = {
      Key: {
        token,
      },
      TableName: this.table,
      UpdateExpression:
        "SET #actualCompletion = :actualCompletion, #result = :result REMOVE #status",
      ConditionExpression: "attribute_exists(#token) AND #lockedBy = :lockedBy",
      ExpressionAttributeNames: {
        "#actualCompletion": "actualCompletion",
        "#result": "result",
        "#status": "status",
        "#lockedBy": "lockedBy",
        "#token": "token",
      },
      ExpressionAttributeValues: {
        ":lockedBy": lockedBy,
        ":actualCompletion": new Date().toISOString(),
        ":result": JSON.stringify(result),
      },
    } as DocumentClient.UpdateItemInput;
    await this.client.update(params).promise();
  }
  private async endWithError(
    token: string,
    lockedBy: string,
    error: string
  ): Promise<void> {
    const params = {
      Key: {
        token,
      },
      TableName: this.table,
      UpdateExpression:
        "SET #actualCompletion = :actualCompletion, #error = :error, #status = :status",
      ConditionExpression: "attribute_exists(#token) AND #lockedBy = :lockedBy",
      ExpressionAttributeNames: {
        "#actualCompletion": "actualCompletion",
        "#error": "error",
        "#status": "status",
        "#lockedBy": "lockedBy",
        "#token": "token",
      },
      ExpressionAttributeValues: {
        ":lockedBy": lockedBy,
        ":actualCompletion": new Date().toISOString(),
        ":error": error,
        ":status": "error",
      },
    } as DocumentClient.UpdateItemInput;
    await this.client.update(params).promise();
  }
  async query(status: Status): Promise<Array<LockRecord>> {
    const params = {
      TableName: this.table,
      IndexName: "byStatus",
      KeyConditionExpression: "#status = :status",
      ExpressionAttributeNames: {
        "#status": "status",
      },
      ExpressionAttributeValues: {
        ":status": status,
      },
    } as DocumentClient.QueryInput;
    const result = await this.client.query(params).promise();
    return result.Items as Array<LockRecord>;
  }
}
