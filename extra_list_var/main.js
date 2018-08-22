'use strict';
require('date-utils');
const express = require('express');
const app = express();
app.set('trust proxy', true);

const PubSub = require('@google-cloud/pubsub');
const projectId = 'MY_PROJECT_ID';

app.get('/b', (req, res) => {
    
    const pubsub = new PubSub({
        projectId: projectId,
    });
    
    var d = new Date();
    var hit_time = d.getTime();
    var hit_date = d.toFormat("YYYY-MM-DD");

    var data = {};
    data["hit_time"] = hit_time.toString();
    data["hit_date"] = hit_date;
    data["hit_id"] = req.query["i"];
    data["listvar"] = req.query["v"];

    var strdata = JSON.stringify(data);
    console.log('data: ' + strdata);

    const dataBuffer = Buffer.from(strdata);

    pubsub
        .topic("projects/MY_PROJECT_ID/topics/my_stats")
        .publisher()
        .publish(dataBuffer)
        .then(results => {
            const messageId = results[0];
            console.log(`Message ${messageId} published.`);
        })
        .catch(err => {
            console.error('ERROR:', err);
        });

    res.status(200)
        .set('Content-Type', 'image/gif').end();

});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log('App listening...');
    console.log('Press Ctrl+C to quit.');
});
