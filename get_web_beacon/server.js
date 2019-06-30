'use strict';
require('date-utils');
const express = require('express');
const app = express();
app.set('trust proxy', true);

const projectId = 'YOUR-PROJECTID-HERE';
const topicName = 'YOUR-TOPIC-NAME';

async function publishMessage(topicName, data) {
  const {PubSub} = require('@google-cloud/pubsub');
  const pubsub = new PubSub({
      projectId: projectId
  });
  const dataBuffer = Buffer.from(data);
  const messageId = await pubsub.topic(topicName).publish(dataBuffer);
}

app.get('/b', (req, res) => {
    
    var d = new Date();
    var hit_time = Math.floor(d.getTime() / 1000);
    var hit_date = d.toFormat("YYYY-MM-DD");

    var data = {};
    data["hit_time"] = hit_time.toString();
    data["hit_date"] = hit_date;
    data["log_type"] = req.query['t'];
    data["log"] = req.query['l'];

    var strdata = JSON.stringify(data);
    publishMessage(topicName, strdata);

    res.status(200)
        .set('Content-Type', 'image/gif').end();

});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log('App listening...');
    console.log('Press Ctrl+C to quit.');
});
