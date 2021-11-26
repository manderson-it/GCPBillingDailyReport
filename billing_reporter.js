const { BigQuery } = require('@google-cloud/bigquery');
/*
const query = (billingAccountId,billingDataset) => `
SELECT
  invoice.month,
  SUM(cost)
    + SUM(IFNULL((SELECT SUM(c.amount)
                  FROM UNNEST(credits) c), 0))
    AS total,
  (SUM(CAST(cost * 1000000 AS int64))
    + SUM(IFNULL((SELECT SUM(CAST(c.amount * 1000000 as int64))
                  FROM UNNEST(credits) c), 0))) / 1000000
    AS total_exact
FROM \`${billingDataset}.gcp_billing_export_resource_v1_${billingAccountId}\`
GROUP BY 1
ORDER BY 1 ASC
;
`;
*/
const query = (billingAccountId,billingDataset) => `
SELECT
  ROUND(SUM(cost),2) AS total,
  FORMAT_DATE('%Y-%m-%d', DATE(usage_end_time)) AS day
FROM \`${billingDataset}.gcp_billing_export_resource_v1_${billingAccountId}\`
WHERE
  cost_type = "regular" AND
  FORMAT_DATE('%Y-%m-%d', DATE(usage_end_time)) = FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()) OR 
  FORMAT_DATE('%Y-%m-%d', DATE(usage_end_time)) = FORMAT_DATE('%Y-%m-%d', DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)) OR
  FORMAT_DATE('%Y-%m-%d', DATE(usage_end_time)) = FORMAT_DATE('%Y-%m-%d', DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY))
GROUP BY 
  day
ORDER BY
  day desc
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
