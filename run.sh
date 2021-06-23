REARC_DATA_PLATFORM_ROLE_ARN='arn:aws:iam::412981388937:role/CrossAccountRole-796406704065-796406704065'
REARC_DATA_PLATFORM_EXTERNAL_ID='Rearc-Data-Platform-796406704065'
ASSET_BUCKET='rearc-data-provider'
MANIFEST_BUCKET='rearc-control-plane-manifest'
CUSTOMER_ID='796406704065'
DATASET_NAME='hud-physical-inspection-scores'
DATASET_ARN='arn:aws:dataexchange:us-east-1:796406704065:data-sets/45902c35a9a89ac1f3a0cc33cdf7f711'
PRODUCT_NAME='HUD USER | Public Housing and Multifamily Physical Inspection Scores'
PRODUCT_ID='prod-wyp62bdnqvpii'
SCHEDULE_CRON="cron(0 8 ? * 1 *)"
REGION='us-east-1'
PROFILE='default'

echo "------------------------------------------------------------------------------"
echo "RearcDataPlatformRoleArn: $REARC_DATA_PLATFORM_ROLE_ARN"
echo "RearcDataPlatformExternalId: $REARC_DATA_PLATFORM_EXTERNAL_ID"
echo "CustomerId: $CUSTOMER_ID"
echo "AssetBucket: $ASSET_BUCKET"
echo "ManifestBucket: $MANIFEST_BUCKET"
echo "DataSetName: $DATASET_NAME"
echo "DataSetArn: $DATASET_ARN"
echo "ProductName: $PRODUCT_NAME"
echo "ProductID: $PRODUCT_ID"
echo "ScheduleCron: $SCHEDULE_CRON"
echo "Region: $REGION"
echo "PROFILE: $PROFILE"
echo "------------------------------------------------------------------------------"


# python pre-processing/pre-processing-code/source_data.py

./init.sh \
    --rdp-role-arn "${REARC_DATA_PLATFORM_ROLE_ARN}" \
    --rdp-external-id "${REARC_DATA_PLATFORM_EXTERNAL_ID}" \
    --customer-id "${CUSTOMER_ID}" \
    --schedule-cron "${SCHEDULE_CRON}" \
    --asset-bucket "${ASSET_BUCKET}" \
    --manifest-bucket "${MANIFEST_BUCKET}" \
    --dataset-name "${DATASET_NAME}" \
    --product-name "${PRODUCT_NAME}" \
    --product-id "${PRODUCT_ID}" \
    --dataset-arn "${DATASET_ARN}" \
    --region "${REGION}" \
    --first-revision "false" \
    --profile "${PROFILE}"
