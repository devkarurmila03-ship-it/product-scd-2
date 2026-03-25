from pydeequ.checks import Check,CheckLevel
from pydeequ.verification import VerificationSuite,VerificationResult

def run_quality_checks(spark,df):
    """
    RUn a handful of core DQ checks. raises valueError in any fail
    """
    check = (
        Check(spark,CheckLevel.Error,"Data quality")
        .hasSize(lambda sz : sz > 0, "non_empty")
        .isComplete("product_id","product_id_not_null")
        .isUnique("product_id","product_id_unique")
    )

    product_check = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(check)
        .run()
    )

    df_check_df = VerificationResult.checkResultsAsDataFrame(spark,product_check)
    df_check_df.show()

    if product_check.status != 'Success':
        raise ValueError("Data Quality checks failed for products data")
    