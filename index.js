const BillingReporter = require('./billing_reporter');
const SlackWebhooker = require('./slack_webhooker');

const PROJECT_ID = process.env.PROJECT_ID;
const BILLING_ACCOUNT_ID = process.env.BILLING_ACCOUNT_ID.replace(/-/g, '_');
const WEBHOOK_URL = process.env.WEBHOOK_URL;
const BILLING_DATASET = process.env.BILLING_DATASET;

function main() {
    const billingReporter = new BillingReporter(
        PROJECT_ID,
        BILLING_ACCOUNT_ID,
        BILLING_DATASET,
        
    );
    const slackWebhooker = new SlackWebhooker(WEBHOOK_URL);
    const dsLink       = "Datastudio link goes here";

    return billingReporter.query().then(
        result => slackWebhooker.post(`:heavy_dollar_sign: Billing Report :heavy_dollar_sign: \n${result} \n${dsLink}`)
    ).then(console.log).catch(console.error);
}

module.exports.main = main;
