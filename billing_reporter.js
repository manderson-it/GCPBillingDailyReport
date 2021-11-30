const { BigQuery } = require('@google-cloud/bigquery');

const query = (billingAccountId,billingDataset) => `
SELECT
  ROUND(SUM(cost),2) AS dailyCost,
  FORMAT_DATE('%Y-%m-%d', DATE(usage_end_time)) AS day,
  (
      SELECT value
      FROM UNNEST(project.labels)
      WHERE KEY='env') AS environment
FROM \`${billingDataset}.gcp_billing_export_resource_v1_${billingAccountId}\`
WHERE
  FORMAT_DATE('%Y-%m-%d', DATE(usage_end_time)) = FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()) OR 
  FORMAT_DATE('%Y-%m-%d', DATE(usage_end_time)) = FORMAT_DATE('%Y-%m-%d', DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)) OR
  FORMAT_DATE('%Y-%m-%d', DATE(usage_end_time)) = FORMAT_DATE('%Y-%m-%d', DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY))
GROUP BY
  environment,
  day
ORDER BY
  day DESC
;
`;
const billingReport = data => data[0].map(row => `${row.day} ${row.total}`).join('\n');

class BillingReporter {

    constructor(projectId, billingAccountId, billingDataset) {
        this.bigquery = new BigQuery({
            projectId: projectId,
        });
        this.billingAccountId = billingAccountId;
        this.billingDataset = billingDataset;
    }

    query() {
        return this.bigquery.query({
            query: query(this.billingAccountId,this.billingDataset),
            useLegacySql: false,
        }).then(data => new Promise(
            resolve => resolve(billingReport(data))
        ));
    }
}

module.exports = BillingReporter;
