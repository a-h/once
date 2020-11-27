import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { Locker } from "./index";

describe("Locker", () => {
  describe("begin and end", () => {
    it("bubbles up errors", async () => {
      const client = new DocumentClient({
        credentials: {
          accessKeyId: "none",
          secretAccessKey: "none",
        },
        region: "eu-west-1",
        endpoint: "http://localhost:8000",
      });
      const tableName = "__invalid_table_name";
      const locker = new Locker(client, tableName, 60);
      try {
        const lock = await locker.begin("abc", "test1", 1000);
        console.log(JSON.stringify(lock));
      } catch (e) {
        expect(e).not.toBeNull();
        expect(e.code).toEqual("ResourceNotFoundException");
      }
      expect.assertions(2);
    });
  });
});
