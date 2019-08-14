-- create Logistic Regression model on BQ ML
CREATE MODEL
  `Models.BreastCancerLogisticRegModel`
OPTIONS
  ( model_type='logistic_reg',labels=['diagnosis'], class_weights=[('B', 0.6), ('M', 0.4)]) AS
SELECT
  *
FROM
  `smu-msds-7346-summer2019-mld.raw_biopsy_data.BCDatasetForModeling`