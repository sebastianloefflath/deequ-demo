from unittest import TestCase

import pydeequ
from pydeequ.analyzers import AnalysisRunner, AnalyzerContext, Completeness, Size
from pydeequ.checks import Check, CheckLevel
from pydeequ.profiles import ColumnProfilerRunner
from pydeequ.suggestions import DEFAULT, ConstraintSuggestionRunner
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import Row
from pyspark.sql.session import SparkSession


class TestDeequ(TestCase):
    def test_something(self):
        #  setup spark session with pydeequ
        spark = (
            SparkSession.Builder()
            .config("spark.jars.packages", pydeequ.deequ_maven_coord)
            .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
            .getOrCreate()
        )

        #  dummy df
        df = spark.createDataFrame(
            [Row(a="foo", b=1, c=5), Row(a="bar", b=2, c=6), Row(a="baz", b=3, c=None)]
        )

        #  pydeequ analyzer
        analysis_result = (
            AnalysisRunner(spark)
            .onData(df)
            .addAnalyzer(Size())
            .addAnalyzer(Completeness("c"))
            .run()
        )

        analysis_result_df = AnalyzerContext.successMetricsAsDataFrame(
            spark, analysis_result
        )
        analysis_result_df.show()

        #  pydeequ profiler
        result = ColumnProfilerRunner(spark).onData(df).run()

        for col, profile in result.profiles.items():
            print(profile)

        #  pydeequ suggestions
        suggestion_result = (
            ConstraintSuggestionRunner(spark)
            .onData(df)
            .addConstraintRule(DEFAULT())
            .run()
        )

        print(suggestion_result)

        #  pydeequ constraint verification
        check = Check(spark, CheckLevel.Warning, "Review Check")

        check_result = (
            VerificationSuite(spark)
            .onData(df)
            .addCheck(
                check.hasSize(lambda x: x >= 3)
                .hasMin("b", lambda x: x == 0)
                .isComplete("c")
                .isUnique("a")
                .isContainedIn("a", ["foo", "bar", "baz"])
                .isNonNegative("b")
            )
            .run()
        )

        check_result_df = VerificationResult.checkResultsAsDataFrame(
            spark, check_result
        )
        check_result_df.show(truncate=60)

        spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

        self.fail()  # so pytest shows the logs
