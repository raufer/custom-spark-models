import unittest
import os
import shutil

from pyspark.ml.linalg import Vectors
from custom_spark_ml.feature.bucketizer import Bucketizer, BucketizerModel
from tests import spark


class TestBucketizer(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        cls.temp_path = '.temp/'

    @classmethod
    def teardown_class(cls):
        pass

    def setUp(self):
        if os.path.exists(self.temp_path):
            shutil.rmtree(self.temp_path)

    def tearDown(self):
        if os.path.exists(self.temp_path):
            shutil.rmtree(self.temp_path)

    def test_model_is_fitting_properly(self):
        """
        'Bucketizer.fit()' return a model with the right fitted 'bins'
        """
        data = [
            (1.0, 0),
            (5.0, 2),
            (0.0, 0),
            (7.0, 2),
            (4.0, 1),
            (8.0, 3),
            (10.0, 4)
        ]

        df = spark.createDataFrame(data, ["input", "expected"])

        nBins = 4

        bucketizer = Bucketizer() \
            .setInputCol("input") \
            .setOutputCol("bin") \
            .setNumberBins(nBins)

        model = bucketizer.fit(df)

        self.assertEqual(1,0)

        self.assertListEqual(
            sorted(model.bins),
            sorted([0.0, 2.5, 5.0, 7.5, 10.0])
        )

    def test_model_is_applying_the_correct_transformation(self):
        """
        Bucketizer.transform should apply the correct transformation
        """
        data = [
            (1.0, 0),
            (5.0, 2),
            (0.0, 0),
            (7.0, 2),
            (4.0, 1),
            (8.0, 3),
            (10.0, 4)
        ]

        df = spark.createDataFrame(data, ["input", "expected"])

        nBins = 4

        bucketizer = Bucketizer() \
            .setInputCol("input") \
            .setOutputCol("bin") \
            .setNumberBins(nBins)

        model = bucketizer.fit(df)

        df_transformed = model.transform(df)

        for row in df_transformed.select('bin', 'expected').collect():
            self.assertEqual(row.bin, row.expected)

    def test_model_persistance(self):
        """
        'Bucketizer.save(path)' should ensure we can load the model back into memory
        """
        data = [
            (1.0, 0),
            (5.0, 2),
            (0.0, 0),
            (7.0, 2),
            (4.0, 1),
            (8.0, 3),
            (10.0, 4)
        ]

        df = spark.createDataFrame(data, ["input", "expected"])

        nBins = 4

        bucketizer = Bucketizer() \
            .setInputCol("input") \
            .setOutputCol("bin") \
            .setNumberBins(nBins)

        model = bucketizer.fit(df)

        df_transformed = model.transform(df)

        model.save(self.temp_path)

        loaded_model = BucketizerModel.load(self.temp_path)
        df_transformed_after_load = loaded_model.transform(df)

        for r1, r2 in zip(df_transformed.collect(), df_transformed_after_load.collect()):
            self.assertEqual(r1.bin, r2.bin)

