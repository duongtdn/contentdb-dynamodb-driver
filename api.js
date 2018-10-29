"use strict"

const AWS = require('aws-sdk')

let dynamodb = null

const table = 'CONTENT';

const Content = {
  TableName : "CONTENT",
  KeySchema: [       
    { AttributeName: "courseId", KeyType: "HASH" }
  ],
  AttributeDefinitions: [       
    { AttributeName: "courseId", AttributeType: "S" }
  ],
  ProvisionedThroughput: {       
      ReadCapacityUnits: 1, 
      WriteCapacityUnits: 1
  }
}

const db = {
  _ready: false,

  createTable(done) {
    if (!this._ready) {
      console.error("DynamoDB is not ready yet")
      return this;
    }

    dynamodb.createTable(Content, function(err, data) {
      if (err) {
        done && done(err);
      } else {
        done && done();
      }
    });

    return this;
  },

  dropTable(done) {
    if (!this._ready) {
      console.error("DynamoDB is not ready yet")
      return this;
    }
    dynamodb.deleteTable({ TableName: table }, done)
  },

  getContent({ courseId }, done) {

    if (!courseId) {
      done && done({error: 'must specify courseId'}, null)
      return
    }
    
    const params = { 
      TableName: table, 
      Key: {
        "courseId": courseId
      }
    }
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.get(params, function(err, data) {
      if (err) {
        done && done({ error:`Unable to read item: ${JSON.stringify(err, null, 2)}`}, null);
      } else {
        if (data && data.Item) {
          done && done(null, data.Item);
        } else {
          done && done(null, null);
        }
      }
    });

  },

  createContent( {uid, content}, done) {

    if (!uid) {
      done && done({error: 'require user id'}, null)
    }

    if (!content) {
      done && done({error: 'empty data'}, null)
    }

    if (!content.courseId) {
      done && done({error: 'missing courseId'}, null)
    }

    if (!content.detail) {
      content.detail = {
        createdBy: uid
      };
    }

    const now = new Date();
    content.detail.createdAt = now.getTime();

    const params = {
      TableName: table,
      Item: content
    };
    
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.put(params, (err, data) => {
      if (err) {
        done && done(err);
      } else {
        done && done();
      }
    });
  },

  removeContent({courseId}, done) {

  },

}

function DynamoDB(onReady) {
 
  dynamodb = new AWS.DynamoDB();

  if (onReady) {
    dynamodb.listTables(function (err, data) {
      if (err) {
        console.log("Error when checking DynamoDB status")
        db._ready = false;
        onReady(err, null);
      } else {
        db._ready = true;
        onReady(null, data);
      }
    });
  } else {
    db._ready = true;
  }

  return db;

}

module.exports = DynamoDB;

