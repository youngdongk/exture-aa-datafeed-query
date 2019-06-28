'use strict';
require('date-utils');
const express = require('express');
const app = express();
app.set('trust proxy', true);

const {PubSub} = require('@google-cloud/pubsub');
const projectId = 'YOUR-PROJECTID-HERE';
const topicName = 'YOUR-TOPIC-NAME';

async function createTopic(topicName) {
  const {PubSub} = require('@google-cloud/pubsub');
  const pubsub = new PubSub();
  await pubsub.createTopic(topicName);
}

app.get('/b', (req, res) => {
    
    const pubsub = new PubSub({
        projectId: projectId,
    });
    
    var d = new Date();
    var hit_time = Math.floor(d.getTime() / 1000);
    var hit_date = d.toFormat("YYYY-MM-DD");

    var data = {};
    data["hit_time"] = hit_time.toString();
    data["hit_date"] = hit_date;
    data["log_type"] = req.query['t'];
    data["log"] = req.query['l'];

    var strdata = JSON.stringify(data);

    const dataBuffer = Buffer.from(strdata);

    createTopic(topicName);

    res.status(200)
        .set('Content-Type', 'image/gif').end();

});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log('App listening...');
    console.log('Press Ctrl+C to quit.');
});
